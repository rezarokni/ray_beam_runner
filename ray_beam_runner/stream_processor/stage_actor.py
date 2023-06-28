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

from typing import List
from typing import Mapping
from typing import Optional

import ray
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability.fn_api_runner import \
  execution as fn_execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import watermark_manager

from ray_beam_runner.stream_processor.state import RayStateManager
from ray_beam_runner.stream_processor.state import \
  PcollectionBufferManager
from ray_beam_runner.stream_processor.ray_pipeline_context import RayStage
from ray_beam_runner.stream_processor.ray_pipeline_context import \
  RayPipelineContext
from ray_beam_runner.stream_processor.ray_pipeline_context import \
  RayWorkerHandlerManager
from ray_beam_runner.stream_processor.sdk_proccess import SDKActor

# TODO Temporary Run method to move the exec stage from the runner code

def run_stage(self):
  pass



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
      pipeline_context: RayPipelineContext = None,

  ) -> None:

    self.stage = stage
    self.pipeline_context = pipeline_context
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
    self.worker_manager = pipeline_context.worker_manager
    self.timer_coder_ids = self._build_timer_coders_id_map()

    self.sdk_process = SDKActor(pipeline_context, stage)
  @property
  def watermark_manager(self):
    # We don't need to wait for this line to execute with ray.get,
    # because any further calls to the watermark manager actor will
    # have to wait for it.
    self._watermark_manager.setup.remote(self.stages)
    return self._watermark_manager

  # def process(self):
  #   bundle_ctx = SDKActor(self, self.stage)
  #   return process_bundles(self, bundle_ctx, self.queue)

  def _build_timer_coders_id_map(self):
    from apache_beam.utils import proto_utils

    timer_coder_ids = {}
    for (
        transform_id,
        transform_proto,
    ) in self.pipeline_context.pipeline_components.transforms.items():
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        pardo_payload = proto_utils.parse_Bytes(
            transform_proto.spec.payload, beam_runner_api_pb2.ParDoPayload
        )
        for id, timer_family_spec in pardo_payload.timer_family_specs.items():
          timer_coder_ids[
            (transform_id, id)
          ] = timer_family_spec.timer_family_coder_id
    return timer_coder_ids


