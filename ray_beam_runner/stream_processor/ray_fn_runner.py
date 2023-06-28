#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A PipelineRunner using the SDK harness."""
# pytype: skip-file
# mypy: check-untyped-defs
import collections
import copy
import logging
import typing
from typing import List
from typing import Tuple
from typing import MutableMapping
from typing import Iterable

from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.pipeline import Pipeline
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.common import group_by_key_input_visitor
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.transforms import environments
from apache_beam.utils import proto_utils, timestamp
from apache_beam.metrics import metric
from apache_beam.metrics.execution import MetricResult
from apache_beam.runners.portability import portable_metrics
from apache_beam.portability.api import metrics_pb2

from ray_beam_runner.stream_processor import utils
from ray_beam_runner.stream_processor.serialization import \
  register_protobuf_serializers
from ray_beam_runner.stream_processor.sdk_proccess import SDKActor
from ray_beam_runner.stream_processor.sdk_proccess import process_bundles
from ray_beam_runner.stream_processor.stage_actor import StageActor
from ray_beam_runner.stream_processor.state import RayStateManager
from ray_beam_runner.stream_processor.temporary_refactor import \
  RayPipelineContext
from ray_beam_runner.stream_processor.temporary_refactor import \
  RayWorkerHandlerManager

_LOGGER = logging.getLogger(__name__)


# This module is experimental. No backwards-compatibility guarantees.


def _setup_options(options: pipeline_options.PipelineOptions):
  """Perform any necessary checkups and updates to input pipeline options"""

  # TODO(pabloem): Add input pipeline options
  RuntimeValueProvider.set_runtime_options({})

  experiments = options.view_as(pipeline_options.DebugOptions).experiments or []
  if "beam_fn_api" not in experiments:
    experiments.append("beam_fn_api")
  options.view_as(pipeline_options.DebugOptions).experiments = experiments


def _check_supported_requirements(
    pipeline_proto: beam_runner_api_pb2.Pipeline,
    supported_requirements: typing.Iterable[str],
):
  """Check that the input pipeline does not have unsuported requirements."""
  for requirement in pipeline_proto.requirements:
    if requirement not in supported_requirements:
      raise ValueError(
          "Unable to run pipeline with requirement: %s" % requirement
      )
  for transform in pipeline_proto.components.transforms.values():
    if transform.spec.urn == common_urns.primitives.TEST_STREAM.urn:
      raise NotImplementedError(transform.spec.urn)
    elif transform.spec.urn in translations.PAR_DO_URNS:
      payload = proto_utils.parse_Bytes(
          transform.spec.payload, beam_runner_api_pb2.ParDoPayload
      )
      for timer in payload.timer_family_specs.values():
        if timer.time_domain != beam_runner_api_pb2.TimeDomain.EVENT_TIME:
          raise NotImplementedError(timer.time_domain)


def _pipeline_checks(
    pipeline: Pipeline,
    options: pipeline_options.PipelineOptions,
    supported_requirements: typing.Iterable[str],
):
  # This is sometimes needed if type checking is disabled
  # to enforce that the inputs (and outputs) of GroupByKey operations
  # are known to be KVs.
  pipeline.visit(
      group_by_key_input_visitor(
          not options.view_as(
              pipeline_options.TypeOptions
          ).allow_non_deterministic_key_coders
      )
  )

  pipeline_proto = pipeline.to_runner_api(
      default_environment=environments.EmbeddedPythonEnvironment.default()
  )
  fn_runner.FnApiRunner._validate_requirements(None, pipeline_proto)

  _check_supported_requirements(pipeline_proto, supported_requirements)
  return pipeline_proto


class RayFnApiRunner(runner.PipelineRunner):
  def __init__(
      self,
      is_drain=False,
  ) -> None:

    """Creates a new Ray Runner instance.

    Args:
      progress_request_frequency: The frequency (in seconds) that the runner
          waits before requesting progress from the SDK.
      is_drain: identify whether expand the sdf graph in the drain mode.
    """
    super().__init__()
    # TODO: figure out if this is necessary (probably, later)
    self._progress_frequency = None
    self._cache_token_generator = fn_runner.FnApiRunner.get_cache_token_generator()
    self._is_drain = is_drain

  @staticmethod
  def supported_requirements():
    # type: () -> Tuple[str, ...]
    return (
        common_urns.requirements.REQUIRES_STATEFUL_PROCESSING.urn,
        common_urns.requirements.REQUIRES_BUNDLE_FINALIZATION.urn,
        common_urns.requirements.REQUIRES_SPLITTABLE_DOFN.urn,
    )

  def run_pipeline(
      self, pipeline: Pipeline, options: pipeline_options.PipelineOptions
  ) -> "RayRunnerResult":

    # Checkup and set up input pipeline options
    _setup_options(options)

    # Check pipeline and convert into protocol buffer representation
    pipeline_proto = _pipeline_checks(
        pipeline, options, self.supported_requirements()
    )

    # Take the protocol buffer representation of the user's pipeline, and
    # apply optimizations.
    stage_context, stages = translations.create_and_optimize_stages(
        copy.deepcopy(pipeline_proto),
        phases=[
            # This is a list of transformations and optimizations to apply
            # to a pipeline.
            translations.annotate_downstream_side_inputs,
            translations.fix_side_input_pcoll_coders,
            translations.pack_combiners,
            translations.lift_combiners,
            translations.expand_sdf,
            translations.expand_gbk,
            translations.sink_flattens,
            translations.greedily_fuse,
            translations.read_to_impulse,
            translations.impulse_to_input,
            translations.sort_stages,
            translations.setup_timer_mapping,
            translations.populate_data_channel_coders,
        ],
        known_runner_urns=frozenset(
            [
                common_urns.primitives.FLATTEN.urn,
                common_urns.primitives.GROUP_BY_KEY.urn,
            ]
        ),
        use_state_iterables=False,
        is_drain=self._is_drain,
    )
    return self.execute_pipeline(stage_context, stages)

  def execute_pipeline(
      self,
      stage_context: translations.TransformContext,
      stages: List[translations.Stage],
  ) -> "RayRunnerResult":
    """Execute pipeline represented by a list of stages and a context."""
    logging.info("Starting pipeline of %d stages." % len(stages))
    dag = utils.assemble_dag(stages)

    register_protobuf_serializers()

    # TODO remove temp extraction of global info
    temp_system_context = RayPipelineContext(stages=stages,
                                             pipeline_components=stage_context.components,
                                             safe_coders=stage_context.safe_coders,
                                             data_channel_coders=stage_context.data_channel_coders)

    # Using this queue to hold 'bundles' that are ready to be processed
    queue = collections.deque()

    # stage metrics
    monitoring_infos_by_stage: MutableMapping[
      str, Iterable["metrics_pb2.MonitoringInfo"]
    ] = {}

    # Global State manager, TODO Change to channels
    state_servicer = RayStateManager()

    # Work manager
    worker_manager = RayWorkerHandlerManager()

    try:
      for stage in stages:
        runner_execution_context = StageActor(
            stage,
            queue,
            state_servicer=state_servicer,
            worker_manager=worker_manager,
            temp_system_context=temp_system_context
        )
        bundle_ctx = SDKActor(runner_execution_context, stage)
        result = process_bundles(
          runner_execution_context=runner_execution_context,
          worker_manager=runner_execution_context.worker_manager,
          pcollection_buffers=runner_execution_context.pcollection_buffers,
          bundle_context_manager=bundle_ctx,
          ready_bundles=queue)
        monitoring_infos_by_stage[
          bundle_ctx.stage.name
        ] = result.process_bundle.monitoring_infos

    finally:
      pass
    return RayRunnerResult(runner.PipelineState.DONE, monitoring_infos_by_stage)


class FnApiMetrics(metric.MetricResults):
  def __init__(self, step_monitoring_infos, user_metrics_only=True):
    """Used for querying metrics from the PipelineResult object.
    step_monitoring_infos: Per step metrics specified as MonitoringInfos.
    user_metrics_only: If true, includes user metrics only.
    """
    self._counters = {}
    self._distributions = {}
    self._gauges = {}
    self._user_metrics_only = user_metrics_only
    self._monitoring_infos = step_monitoring_infos

    for smi in step_monitoring_infos.values():
      counters, distributions, gauges = portable_metrics.from_monitoring_infos(
          smi, user_metrics_only
      )
      self._counters.update(counters)
      self._distributions.update(distributions)
      self._gauges.update(gauges)

  def query(self, filter=None):
    counters = [
        MetricResult(k, v, v)
        for k, v in self._counters.items()
        if self.matches(filter, k)
    ]
    distributions = [
        MetricResult(k, v, v)
        for k, v in self._distributions.items()
        if self.matches(filter, k)
    ]
    gauges = [
        MetricResult(k, v, v)
        for k, v in self._gauges.items()
        if self.matches(filter, k)
    ]

    return {
        self.COUNTERS: counters,
        self.DISTRIBUTIONS: distributions,
        self.GAUGES: gauges,
    }

  def monitoring_infos(self):
    # type: () -> List[metrics_pb2.MonitoringInfo]
    return [item for sublist in self._monitoring_infos.values() for item in
            sublist]


class RayRunnerResult(runner.PipelineResult):
  def __init__(self, state, monitoring_infos_by_stage):
    super().__init__(state)
    self._monitoring_infos_by_stage = monitoring_infos_by_stage
    self._metrics = None
    self._monitoring_metrics = None

  def wait_until_finish(self, duration=None):
    return None

  def metrics(self):
    """Returns a queryable object including user metrics only."""
    if self._metrics is None:
      self._metrics = FnApiMetrics(
          self._monitoring_infos_by_stage, user_metrics_only=True
      )
    return self._metrics

  def monitoring_metrics(self):
    """Returns a queryable object including all metrics."""
    if self._monitoring_metrics is None:
      self._monitoring_metrics = FnApiMetrics(
          self._monitoring_infos_by_stage, user_metrics_only=False
      )
    return self._monitoring_metrics
