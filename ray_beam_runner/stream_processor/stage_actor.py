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


import collections
import dataclasses
import logging
import random
from typing import List
from typing import Mapping
from typing import Optional

import ray
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability.fn_api_runner import \
  execution as fn_execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import watermark_manager

from ray_beam_runner.portability.execution import PcollectionBufferManager
from ray_beam_runner.portability.execution import RayWorkerHandlerManager
from ray_beam_runner.stream_processor.state import RayStateManager
from ray_beam_runner.stream_processor.temporary_refactor import RayStage
from ray_beam_runner.stream_processor.temporary_refactor import \
  RayPipelineContext




# TODO Temporary Run method to move the exec stage from the runner code

def run_stage(self):
  pass


def _get_input_id(buffer_id, transform_name):
  """Get the 'buffer_id' for the input data we're retrieving.

  For most data, the buffer ID is as expected, but for IMPULSE readers, the
  buffer ID is the consumer name.
  """
  if isinstance(buffer_id, bytes) and (
      buffer_id.startswith(b"materialize")
      or buffer_id.startswith(b"timer")
      or buffer_id.startswith(b"group")
  ):
    buffer_id = buffer_id
  else:
    buffer_id = transform_name.encode("ascii")
  return buffer_id


@ray.remote
class _RayRunnerStats:
  def __init__(self):
    self._bundle_uid = 0

  def next_bundle(self):
    self._bundle_uid += 1
    return self._bundle_uid

@ray.remote
class RayWatermarkManager(watermark_manager.WatermarkManager):
  def __init__(self):
    # the original WatermarkManager performs a lot of computation
    # in its __init__ method. Because Ray calls __init__ whenever
    # it deserializes an object, we'll move its setup elsewhere.
    self._initialized = False
    self._pcollections_by_name = {}
    self._stages_by_name = {}

  def setup(self, stages):
    if self._initialized:
      return
    logging.debug("initialized the RayWatermarkManager")
    self._initialized = True
    watermark_manager.WatermarkManager.setup(self, stages)


class StageActor(object):
  def __init__(
      self,
      stage: RayStage,
      queue: collections.deque,
      state_servicer: Optional[RayStateManager],
      worker_manager: Optional[RayWorkerHandlerManager] = None,
      pcollection_buffers: PcollectionBufferManager = None,
      temp_system_context: RayPipelineContext = None,

  ) -> None:

    self.stage = stage
    self.queue = queue
    self.temp_system_context = temp_system_context
    self.pcollection_buffers = pcollection_buffers or PcollectionBufferManager()
    self.state_servicer = state_servicer
    self._watermark_manager = RayWatermarkManager.remote()
    # self.pipeline_context = pipeline_context.PipelineContext(
    #   pipeline_components)
    # self.safe_windowing_strategies = {
    #     # TODO: Enable safe_windowing_strategy after
    #     #  figuring out how to pickle the function.
    #     # id: self._make_safe_windowing_strategy(id)
    #     id: id
    #     for id in pipeline_components.windowing_strategies.keys()
    # }
    self.stats = _RayRunnerStats.remote()
    self._uid = 0
    self.worker_manager = worker_manager or RayWorkerHandlerManager()
    self.timer_coder_ids = self._build_timer_coders_id_map()
    self.encoded_impulse_ref = ray.put([fn_execution.ENCODED_IMPULSE_VALUE])

  @property
  def watermark_manager(self):
    # We don't need to wait for this line to execute with ray.get,
    # because any further calls to the watermark manager actor will
    # have to wait for it.
    self._watermark_manager.setup.remote(self.stages)
    return self._watermark_manager

  @staticmethod
  def next_uid():
    # TODO(pabloem): Use stats actor for UIDs.
    # return str(ray.get(self.stats.next_bundle.remote()))
    # self._uid += 1
    return str(random.randint(0, 11111111))

  # def process(self):
  #   bundle_ctx = SDKActor(self, self.stage)
  #   return process_bundles(self, bundle_ctx, self.queue)

  def _build_timer_coders_id_map(self):
    from apache_beam.utils import proto_utils

    timer_coder_ids = {}
    for (
        transform_id,
        transform_proto,
    ) in self.temp_system_context.pipeline_components.transforms.items():
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        pardo_payload = proto_utils.parse_Bytes(
            transform_proto.spec.payload, beam_runner_api_pb2.ParDoPayload
        )
        for id, timer_family_spec in pardo_payload.timer_family_specs.items():
          timer_coder_ids[
            (transform_id, id)
          ] = timer_family_spec.timer_family_coder_id
    return timer_coder_ids

  def commit_side_inputs_to_state(self,
      data_side_input: translations.DataSideInput):
    """
    Store side inputs in the state manager so that they can be accessed by workers.
    """
    for (consuming_transform_id, tag), (
        buffer_id,
        func_spec,
    ) in data_side_input.items():
      _, pcoll_id = translations.split_buffer_id(buffer_id)
      value_coder = self.temp_system_context.pipeline_context.coders[
        self.temp_system_context.safe_coders[self.temp_system_context.data_channel_coders[pcoll_id]]
      ]

      elements_by_window = fn_execution.WindowGroupingBuffer(
          func_spec, value_coder
      )

      # TODO: Fix this
      pcoll_buffer = ray.get(self.pcollection_buffers.get(buffer_id))
      for bundle_items in pcoll_buffer:
        for bundle_item in bundle_items:
          elements_by_window.append(bundle_item)

      futures = []
      if func_spec.urn == common_urns.side_inputs.ITERABLE.urn:
        for _, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window,
              )
          )
          futures.append(
              self.state_servicer.append_raw(
                  state_key, elements_data
              )._object_ref
          )
      elif func_spec.urn == common_urns.side_inputs.MULTIMAP.urn:
        for key, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window,
                  key=key,
              )
          )
          futures.append(
              self.state_servicer.append_raw(
                  state_key, elements_data
              )._object_ref
          )
      else:
        raise ValueError("Unknown access pattern: '%s'" % func_spec.urn)

      ray.wait(futures, num_returns=len(futures))


@dataclasses.dataclass
class Bundle:
  input_timers: Mapping[
    translations.TimerFamilyId, fn_execution.PartitionableBuffer]
  input_data: Mapping[str, List[ray.ObjectRef]]
