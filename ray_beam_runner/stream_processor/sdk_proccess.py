# pytype: skip-file
# mypy: check-untyped-defs
import collections
import logging
import typing
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union
from typing import Generator

import apache_beam
from apache_beam import coders

import ray
from apache_beam import beam_runner_api_pb2
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability.fn_api_runner import execution
from apache_beam.runners.portability.fn_api_runner import \
  execution as fn_execution
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import worker_handlers
from apache_beam.runners.portability.fn_api_runner.execution import ListBuffer
from apache_beam.runners.portability.fn_api_runner.execution import \
  PartitionableBuffer
from apache_beam.runners.portability.fn_api_runner.translations import \
  DataOutput
from apache_beam.runners.portability.fn_api_runner.translations import \
  TimerFamilyId
from apache_beam.runners.worker import bundle_processor
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp
from apache_beam.portability import common_urns

from ray_beam_runner.stream_processor.serialization import \
  register_protobuf_serializers
from ray_beam_runner.stream_processor.stage_actor import Bundle
from ray_beam_runner.stream_processor.stage_actor import StageActor
from ray_beam_runner.stream_processor.stage_actor import _get_input_id
from ray_beam_runner.stream_processor.state import RayStateManager
from ray_beam_runner.stream_processor.temporary_refactor import \
  PcollectionBufferManager
from ray_beam_runner.stream_processor.temporary_refactor import \
  RayPipelineContext
from ray_beam_runner.stream_processor.temporary_refactor import \
  RayWorkerHandlerManager

from ray_beam_runner.stream_processor.temporary_refactor import \
  merge_stage_results

_LOGGER = logging.getLogger(__name__)


def _get_source_transform_name(
    process_bundle_descriptor: beam_fn_api_pb2.ProcessBundleDescriptor,
    transform_id: str,
    input_id: str,
) -> str:
  """Find the name of the source PTransform that feeds into the given
  (transform_id, input_id)."""
  input_pcoll = process_bundle_descriptor.transforms[transform_id].inputs[
    input_id]
  for ptransform_id, ptransform in process_bundle_descriptor.transforms.items():
    # The GrpcRead is directly followed by the SDF/Process.
    if (
        ptransform.spec.urn == bundle_processor.DATA_INPUT_URN
        and input_pcoll in ptransform.outputs.values()
    ):
      return ptransform_id

    # The GrpcRead is followed by SDF/Truncate -> SDF/Process.
    # We need to traverse the TRUNCATE_SIZED_RESTRICTION node in order
    # to find the original source PTransform.
    if (
        ptransform.spec.urn
        == common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn
        and input_pcoll in ptransform.outputs.values()
    ):
      input_pcoll_ = translations.only_element(
          process_bundle_descriptor.transforms[ptransform_id].inputs.values()
      )
      for (
          ptransform_id_2,
          ptransform_2,
      ) in process_bundle_descriptor.transforms.items():
        if (
            ptransform_2.spec.urn == bundle_processor.DATA_INPUT_URN
            and input_pcoll_ in ptransform_2.outputs.values()
        ):
          return ptransform_id_2

  raise RuntimeError("No IO transform feeds %s" % transform_id)


def _retrieve_delayed_applications(
    bundle_result: beam_fn_api_pb2.InstructionResponse,
    process_bundle_descriptor: beam_fn_api_pb2.ProcessBundleDescriptor,
):
  """Extract delayed applications from a bundle run.

  A delayed application represents a user-initiated checkpoint, where user code
  delays the consumption of a data element to checkpoint the previous elements
  in a bundle.
  """
  delayed_bundles = {}
  for delayed_application in bundle_result.process_bundle.residual_roots:
    # TODO(pabloem): Time delay needed for streaming. For now we'll ignore it.
    # time_delay = delayed_application.requested_time_delay
    source_transform = _get_source_transform_name(
        process_bundle_descriptor,
        delayed_application.application.transform_id,
        delayed_application.application.input_id,
    )

    if source_transform not in delayed_bundles:
      delayed_bundles[source_transform] = []
    delayed_bundles[source_transform].append(
        delayed_application.application.element
    )

  for consumer, data in delayed_bundles.items():
    delayed_bundles[consumer] = [data]

  return delayed_bundles


def _fetch_decode_data(
    runner_context: RayPipelineContext,
    buffer_id: bytes,
    coder_id: str,
    data_references: List[ray.ObjectRef],
):
  """Fetch a PCollection's data and decode it."""
  if buffer_id.startswith(b"group"):
    _, pcoll_id = translations.split_buffer_id(buffer_id)
    transform = \
    runner_context.pipeline_components.transforms[pcoll_id]
    out_pcoll = \
    runner_context.pipeline_components.pcollections[
      translations.only_element(transform.outputs.values())
    ]
    windowing_strategy = \
      runner_context.pipeline_components.windowing_strategies[
        out_pcoll.windowing_strategy_id
      ]
    postcoder = runner_context.pipeline_context.coders[
      coder_id]
    precoder = coders.WindowedValueCoder(
        coders.TupleCoder(
            (
                postcoder.wrapped_value_coder._coders[0],
                postcoder.wrapped_value_coder._coders[1]._elem_coder,
            )
        ),
        postcoder.window_coder,
    )
    buffer = fn_execution.GroupingBuffer(
        pre_grouped_coder=precoder,
        post_grouped_coder=postcoder,
        windowing=apache_beam.Windowing.from_runner_api(windowing_strategy,
                                                        None),
    )
  else:
    buffer = fn_execution.ListBuffer(
        coder_impl=runner_context.pipeline_context.coders[
          coder_id].get_impl()
    )

  for block in ray.get(data_references):
    # TODO(pabloem): Stop using ListBuffer, and use different
    #  buffers to pass data to Beam.
    for elm in block:
      buffer.append(elm)
  return buffer


def _send_timers(
    worker_handler: worker_handlers.WorkerHandler,
    input_bundle: "Bundle",
    stage_timers: Mapping[translations.TimerFamilyId, bytes],
    process_bundle_id,
) -> None:
  """Pass timers to the worker for processing."""
  for transform_id, timer_family_id in stage_timers.keys():
    timer_out = worker_handler.data_conn.output_timer_stream(
        process_bundle_id, transform_id, timer_family_id
    )
    for timer in input_bundle.input_timers.get((transform_id, timer_family_id),
                                               []):
      timer_out.write(timer)
    timer_out.close()


def _get_worker_handler(
    state_servicer: RayStateManager,
    worker_manager: RayWorkerHandlerManager,
    bundle_descriptor_id
) -> worker_handlers.WorkerHandler:
  worker_handler = worker_handlers.EmbeddedWorkerHandler(
      None,  # Unnecessary payload.
      state_servicer,
      None,  # Unnecessary provision info.
      worker_manager,
  )
  worker_handler.worker.bundle_processor_cache.register(
      worker_manager.process_bundle_descriptor(
          bundle_descriptor_id)
  )
  return worker_handler


@ray.remote(num_returns="dynamic")
def ray_execute_bundle(
    runner_context: "StageActor",
    input_bundle: "Bundle",
    transform_buffer_coder: Mapping[str, typing.Tuple[bytes, str]],
    expected_outputs: translations.DataOutput,
    stage_timers: Mapping[translations.TimerFamilyId, bytes],
    instruction_request_repr: Mapping[str, typing.Any],
    dry_run=False,
) -> Generator:
  # generator returns:
  # (serialized InstructionResponse, ouputs,
  #  repeat of pcoll, data,
  #  delayed applications, repeat of pcoll, data)

  register_protobuf_serializers()
  instruction_request = beam_fn_api_pb2.InstructionRequest(
      instruction_id=instruction_request_repr["instruction_id"],
      process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
          process_bundle_descriptor_id=instruction_request_repr[
            "process_descriptor_id"
          ],
          cache_tokens=[instruction_request_repr["cache_token"]],
      ),
  )
  output_buffers: Mapping[
    typing.Union[str, translations.TimerFamilyId], list
  ] = collections.defaultdict(list)
  process_bundle_id = instruction_request.instruction_id

  worker_handler = _get_worker_handler(
      runner_context.state_servicer, runner_context.worker_manager,
      instruction_request_repr["process_descriptor_id"]
  )

  _send_timers(worker_handler, input_bundle, stage_timers, process_bundle_id)

  input_data = {
      k: _fetch_decode_data(
          runner_context.temp_system_context,
          _get_input_id(transform_buffer_coder[k][0], k),
          transform_buffer_coder[k][1],
          objrefs,
      )
      for k, objrefs in input_bundle.input_data.items()
  }

  for transform_id, elements in input_data.items():
    data_out = worker_handler.data_conn.output_stream(
        process_bundle_id, transform_id
    )
    for byte_stream in elements:
      data_out.write(byte_stream)
    data_out.close()

  expect_reads: List[typing.Union[str, translations.TimerFamilyId]] = list(
      expected_outputs.keys()
  )
  expect_reads.extend(list(stage_timers.keys()))

  result_future = worker_handler.control_conn.push(instruction_request)

  for output in worker_handler.data_conn.input_elements(
      process_bundle_id,
      expect_reads,
      abort_callback=lambda: (
          result_future.is_done() and bool(result_future.get().error)
      ),
  ):
    if isinstance(output, beam_fn_api_pb2.Elements.Timers) and not dry_run:
      output_buffers[
        stage_timers[(output.transform_id, output.timer_family_id)]
      ].append(output.timers)
    if isinstance(output, beam_fn_api_pb2.Elements.Data) and not dry_run:
      output_buffers[expected_outputs[output.transform_id]].append(output.data)

  result: beam_fn_api_pb2.InstructionResponse = result_future.get()

  if result.process_bundle.requires_finalization:
    finalize_request = beam_fn_api_pb2.InstructionRequest(
        finalize_bundle=beam_fn_api_pb2.FinalizeBundleRequest(
            instruction_id=process_bundle_id
        )
    )
    finalize_response = worker_handler.control_conn.push(finalize_request).get()
    if finalize_response.error:
      raise RuntimeError(finalize_response.error)

  returns = [result]

  returns.append(len(output_buffers))
  for pcoll, buffer in output_buffers.items():
    returns.append(pcoll)
    returns.append(buffer)

  # Now we collect all the deferred inputs remaining from bundle execution.
  # Deferred inputs can be:
  # - timers
  # - SDK-initiated deferred applications of root elements
  # - # TODO: Runner-initiated deferred applications of root elements
  process_bundle_descriptor = runner_context.worker_manager.process_bundle_descriptor(
      instruction_request_repr["process_descriptor_id"]
  )
  delayed_applications = _retrieve_delayed_applications(
      result,
      process_bundle_descriptor
  )

  returns.append(len(delayed_applications))
  for pcoll, buffer in delayed_applications.items():
    returns.append(pcoll)
    returns.append(buffer)

  for ret in returns:
    yield ret


class SDKActor:
  def __init__(
      self,
      execution_context: StageActor,
      stage: translations.Stage,
  ) -> None:
    self.execution_context = execution_context
    self.stage = stage
    # self.extract_bundle_inputs_and_outputs()
    self.bundle_uid = self.execution_context.next_uid()

    # Properties that are lazily initialized
    self._process_bundle_descriptor = (
        None
    )  # type: Optional[beam_fn_api_pb2.ProcessBundleDescriptor]
    self._worker_handlers = (
        None
    )  # type: Optional[List[worker_handlers.WorkerHandler]]
    # a mapping of {(transform_id, timer_family_id): timer_coder_id}. The map
    # is built after self._process_bundle_descriptor is initialized.
    # This field can be used to tell whether current bundle has timers.
    self._timer_coder_ids = None  # type: Optional[Dict[Tuple[str, str], str]]

  def __reduce__(self):
    data = (self.execution_context, self.stage)

    def deserializer(args):
      SDKActor(args[0], args[1])

    return (deserializer, data)

  @property
  def worker_handlers(self) -> List[worker_handlers.WorkerHandler]:
    return []

  def data_api_service_descriptor(
      self,
  ) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
    return endpoints_pb2.ApiServiceDescriptor(url="fake")

  def state_api_service_descriptor(
      self,
  ) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
    return None

  @property
  def process_bundle_descriptor(
      self) -> beam_fn_api_pb2.ProcessBundleDescriptor:
    if self._process_bundle_descriptor is None:
      self._process_bundle_descriptor = (
          beam_fn_api_pb2.ProcessBundleDescriptor.FromString(
              self._build_process_bundle_descriptor()
          )
      )
      self._timer_coder_ids = (
          fn_execution.BundleContextManager._build_timer_coders_id_map(self)
      )
    return self._process_bundle_descriptor

  def _build_process_bundle_descriptor(self):
    # Cannot be invoked until *after* _extract_endpoints is called.
    # Always populate the timer_api_service_descriptor.
    pbd = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self.bundle_uid,
        transforms={
            transform.unique_name: transform for transform in
            self.stage.transforms
        },
        pcollections=dict(
            self.execution_context.temp_system_context.pipeline_components.pcollections.items()
        ),
        coders=dict(
          self.execution_context.temp_system_context.pipeline_components.coders.items()),
        windowing_strategies=dict(
            self.execution_context.temp_system_context.pipeline_components.windowing_strategies.items()
        ),
        environments=dict(
            self.execution_context.temp_system_context.pipeline_components.environments.items()
        ),
        state_api_service_descriptor=self.state_api_service_descriptor(),
        timer_api_service_descriptor=self.data_api_service_descriptor(),
    )

    return pbd.SerializeToString()

  def get_bundle_inputs_and_outputs(
      self,
  ) -> Tuple[
    Dict[str, PartitionableBuffer], DataOutput, Dict[TimerFamilyId, bytes]]:
    """Returns maps of transform names to PCollection identifiers.

    Also mutates IO stages to point to the data ApiServiceDescriptor.

    Returns:
      A tuple of (data_input, data_output, expected_timer_output) dictionaries.
        `data_input` is a dictionary mapping (transform_name, output_name) to a
        PCollection buffer; `data_output` is a dictionary mapping
        (transform_name, output_name) to a PCollection ID.
        `expected_timer_output` is a dictionary mapping transform_id and
        timer family ID to a buffer id for timers.
    """
    return self.transform_to_buffer_coder, self.data_output, self.stage_timers

  def setup(self):
    transform_to_buffer_coder: typing.Dict[str, typing.Tuple[bytes, str]] = {}
    data_output = {}  # type: DataOutput
    expected_timer_output = {}  # type: OutputTimers
    for transform in self.stage.transforms:
      if transform.spec.urn in (
          bundle_processor.DATA_INPUT_URN,
          bundle_processor.DATA_OUTPUT_URN,
      ):
        pcoll_id = transform.spec.payload
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          coder_id = \
          self.execution_context.temp_system_context.data_channel_coders[
            translations.only_element(transform.outputs.values())
          ]
          if pcoll_id == translations.IMPULSE_BUFFER:
            pcoll_id = transform.unique_name.encode("utf8")
            self.execution_context.pcollection_buffers.put(
                pcoll_id, [self.execution_context.encoded_impulse_ref]
            )
          else:
            pass
          transform_to_buffer_coder[transform.unique_name] = (
              pcoll_id,
              self.execution_context.temp_system_context.safe_coders.get(
                coder_id, coder_id),
          )
        elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          data_output[transform.unique_name] = pcoll_id
          coder_id = \
          self.execution_context.temp_system_context.data_channel_coders[
            translations.only_element(transform.inputs.values())
          ]
        else:
          raise NotImplementedError
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        transform.spec.payload = data_spec.SerializeToString()
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload
        )
        for timer_family_id in payload.timer_family_specs.keys():
          expected_timer_output[
            (transform.unique_name, timer_family_id)
          ] = translations.create_buffer_id(timer_family_id, "timers")
    self.transform_to_buffer_coder, self.data_output, self.stage_timers = (
        transform_to_buffer_coder,
        data_output,
        expected_timer_output,
    )


def process_bundles(
    runner_execution_context: StageActor,
    worker_manager: RayWorkerHandlerManager,
    pcollection_buffers: PcollectionBufferManager,
    bundle_context_manager: SDKActor,
    ready_bundles: collections.deque,
) -> beam_fn_api_pb2.InstructionResponse:
  """Run an individual stage.

  Args:
    runner_execution_context: An object containing execution information for
      the pipeline.
    bundle_context_manager (execution.BundleContextManager): A description of
      the stage to execute, and its context.
  """
  bundle_context_manager.setup()
  worker_manager.register_process_bundle_descriptor(
      bundle_context_manager.process_bundle_descriptor
  )
  input_timers: Mapping[
    translations.TimerFamilyId, execution.PartitionableBuffer
  ] = {}

  input_data = {
      k: pcollection_buffers.get(
          _get_input_id(bundle_context_manager.transform_to_buffer_coder[k][0],
                        k)
      )
      for k in bundle_context_manager.transform_to_buffer_coder
  }

  final_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]

  while True:
    (
        last_result,
        fired_timers,
        delayed_applications,
        bundle_outputs,
    ) = _run_bundle(
        runner_execution_context,
        bundle_context_manager,
        Bundle(input_timers=input_timers, input_data=input_data),
    )

    final_result = merge_stage_results(final_result, last_result)
    if not delayed_applications and not fired_timers:
      break
    else:
      # TODO: Enable following assertion after watermarking is implemented
      # assert (ray.get(
      # runner_execution_context.watermark_manager
      # .get_stage_node.remote(
      #     bundle_context_manager.stage.name)).output_watermark()
      #         < timestamp.MAX_TIMESTAMP), (
      #     'wrong timestamp for %s. '
      #     % ray.get(
      #     runner_execution_context.watermark_manager
      #     .get_stage_node.remote(
      #     bundle_context_manager.stage.name)))
      input_data = delayed_applications
      input_timers = fired_timers

  # Store the required downstream side inputs into state so it is accessible
  # for the worker when it runs bundles that consume this stage's output.
  data_side_input = runner_execution_context.temp_system_context.side_input_descriptors_by_stage.get(
      bundle_context_manager.stage.name, {}
  )
  runner_execution_context.commit_side_inputs_to_state(data_side_input)

  return final_result


def _run_bundle(
    runner_execution_context: StageActor,
    bundle_context_manager: SDKActor,
    input_bundle: Bundle,
) -> Tuple[
  beam_fn_api_pb2.InstructionResponse,
  Dict[translations.TimerFamilyId, ListBuffer],
  Mapping[str, ray.ObjectRef],
  List[Union[str, translations.TimerFamilyId]],
]:
  """Execute a bundle, and return a result object, and deferred inputs."""
  (
      transform_to_buffer_coder,
      data_output,
      stage_timers,
  ) = bundle_context_manager.get_bundle_inputs_and_outputs()

  cache_token_generator = fn_runner.FnApiRunner.get_cache_token_generator(
      static=False
  )

  process_bundle_descriptor = bundle_context_manager.process_bundle_descriptor

  # TODO(pabloem): Are there two different IDs? the Bundle ID and PBD ID?
  process_bundle_id = "bundle_%s" % process_bundle_descriptor.id

  pbd_id = process_bundle_descriptor.id
  result_generator_ref = ray_execute_bundle.remote(
      runner_execution_context,
      input_bundle,
      transform_to_buffer_coder,
      data_output,
      stage_timers,
      instruction_request_repr={
          "instruction_id": process_bundle_id,
          "process_descriptor_id": pbd_id,
          "cache_token": next(cache_token_generator),
      },
  )
  result_generator = iter(ray.get(result_generator_ref))
  result = ray.get(next(result_generator))

  output = []
  num_outputs = ray.get(next(result_generator))
  for _ in range(num_outputs):
    pcoll = ray.get(next(result_generator))
    data_ref = next(result_generator)
    output.append(pcoll)
    runner_execution_context.pcollection_buffers.put(pcoll, [data_ref])

  delayed_applications = {}
  num_delayed_applications = ray.get(next(result_generator))
  for _ in range(num_delayed_applications):
    pcoll = ray.get(next(result_generator))
    data_ref = next(result_generator)
    delayed_applications[pcoll] = data_ref
    runner_execution_context.pcollection_buffers.put(pcoll, [data_ref])

  (
      watermarks_by_transform_and_timer_family,
      newly_set_timers,
  ) = _collect_written_timers(bundle_context_manager)

  # TODO(pabloem): Add support for splitting of results.

  # After collecting deferred inputs, we 'pad' the structure with empty
  # buffers for other expected inputs.
  # if deferred_inputs or newly_set_timers:
  #   # The worker will be waiting on these inputs as well.
  #   for other_input in data_input:
  #     if other_input not in deferred_inputs:
  #       deferred_inputs[other_input] = ListBuffer(
  #           coder_impl=bundle_context_manager.get_input_coder_impl(
  #               other_input))

  return result, newly_set_timers, delayed_applications, output


@staticmethod
def _collect_written_timers(
    bundle_context_manager: SDKActor,
) -> Tuple[
  Dict[translations.TimerFamilyId, timestamp.Timestamp],
  Mapping[translations.TimerFamilyId, execution.PartitionableBuffer],
]:
  """Review output buffers, and collect written timers.
  This function reviews a stage that has just been run. The stage will have
  written timers to its output buffers. The function then takes the timers,
  and adds them to the `newly_set_timers` dictionary, and the
  timer_watermark_data dictionary.
  The function then returns the following two elements in a tuple:
  - timer_watermark_data: A dictionary mapping timer family to upcoming
      timestamp to fire.
  - newly_set_timers: A dictionary mapping timer family to timer buffers
      to be passed to the SDK upon firing.
  """
  timer_watermark_data = {}
  newly_set_timers = {}

  execution_context = bundle_context_manager.execution_context
  buffer_manager = execution_context.pcollection_buffers

  for (
      transform_id,
      timer_family_id,
  ), buffer_id in bundle_context_manager.stage_timers.items():
    timer_buffer = buffer_manager.get(buffer_id)

    coder_id = bundle_context_manager._timer_coder_ids[
      (transform_id, timer_family_id)
    ]

    coder = execution_context.temp_system_context.pipeline_context.coders[
      coder_id]
    timer_coder_impl = coder.get_impl()

    timers_by_key_tag_and_window = {}
    if len(timer_buffer) >= 1:
      written_timers = ray.get(timer_buffer[0])
      # clear the timer buffer
      buffer_manager.clear(buffer_id)

      # deduplicate updates to the same timer
      for elements_timers in written_timers:
        for decoded_timer in timer_coder_impl.decode_all(elements_timers):
          key_tag_win = (
              decoded_timer.user_key,
              decoded_timer.dynamic_timer_tag,
              decoded_timer.windows[0],
          )
          if not decoded_timer.clear_bit:
            timers_by_key_tag_and_window[key_tag_win] = decoded_timer
          elif (
              decoded_timer.clear_bit
              and key_tag_win in timers_by_key_tag_and_window
          ):
            del timers_by_key_tag_and_window[key_tag_win]
    if not timers_by_key_tag_and_window:
      continue

    out = create_OutputStream()
    for decoded_timer in timers_by_key_tag_and_window.values():
      timer_coder_impl.encode_to_stream(decoded_timer, out, True)
      timer_watermark_data[(transform_id, timer_family_id)] = min(
          timer_watermark_data.get(
              (transform_id, timer_family_id), timestamp.MAX_TIMESTAMP
          ),
          decoded_timer.hold_timestamp,
      )

    buf = ListBuffer(coder_impl=timer_coder_impl)
    buf.append(out.get())
    newly_set_timers[(transform_id, timer_family_id)] = buf
  return timer_watermark_data, newly_set_timers
