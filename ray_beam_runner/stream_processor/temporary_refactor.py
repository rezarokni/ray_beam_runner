import collections
import itertools
from typing import List, Mapping

import ray
from apache_beam.metrics import monitoring_infos
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context


from apache_beam.runners.portability.fn_api_runner import \
  execution as fn_execution

from apache_beam.runners.portability.fn_api_runner import translations


class RayStage(translations.Stage):
    @staticmethod
    def from_Stage(stage: translations.Stage):
        return RayStage(
            stage.name,
            stage.transforms,
            stage.downstream_side_inputs,
            # stage.must_follow,
            [],
            stage.parent,
            stage.environment,
            stage.forced_root,
      )

class RayWorkerHandlerManager:
    def __init__(self):
        self._process_bundle_descriptors = {}

    def register_process_bundle_descriptor(self, process_bundle_descriptor):
        ray_process_bundle_descriptor = process_bundle_descriptor
        self._process_bundle_descriptors[
            ray_process_bundle_descriptor.id
        ] = ray_process_bundle_descriptor

    def process_bundle_descriptor(self, id):
        return self._process_bundle_descriptors[id]

class PcollectionBufferManager:
    def __init__(self):
        self.buffers = collections.defaultdict(list)

    def put(self, pcoll, data_refs: List[ray.ObjectRef]):
        self.buffers[pcoll].extend(data_refs)

    def get(self, pcoll) -> List[ray.ObjectRef]:
        return self.buffers[pcoll]

    def clear(self, pcoll):
        self.buffers[pcoll].clear()


class RayPipelineContext(object):
    """ Temporaray Refectoring utilty to move out later """

    def __init__(
        self,
        stages: List[translations.Stage],
        pipeline_components: beam_runner_api_pb2.Components,
        safe_coders: translations.SafeCoderMapping,
        data_channel_coders: Mapping[str, str],

    ):
        self.stages = [
            RayStage.from_Stage(s) if not isinstance(s, RayStage) else s for
            s in stages
        ]

        self.side_input_descriptors_by_stage = (
            fn_execution
                .FnApiRunnerExecutionContext._build_data_side_inputs_map(
                stages)
        )
        self.pipeline_components = pipeline_components
        self.safe_coders = safe_coders
        self.data_channel_coders = data_channel_coders
        self.pipeline_context = pipeline_context.PipelineContext(
            pipeline_components)
        self.safe_windowing_strategies = {
            # TODO: Enable safe_windowing_strategy after
            #  figuring out how to pickle the function.
            # id: self._make_safe_windowing_strategy(id)
            id: id
            for id in pipeline_components.windowing_strategies.keys()
}

def merge_stage_results(
    previous_result: beam_fn_api_pb2.InstructionResponse,
    last_result: beam_fn_api_pb2.InstructionResponse,
) -> beam_fn_api_pb2.InstructionResponse:
    """Merge InstructionResponse objects from executions of same stage bundles.

    This method is used to produce a global per-stage result object with
    aggregated metrics and results.
    """
    return (
        last_result
        if previous_result is None
        else beam_fn_api_pb2.InstructionResponse(
            process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                monitoring_infos=monitoring_infos.consolidate(
                    itertools.chain(
                        previous_result.process_bundle.monitoring_infos,
                        last_result.process_bundle.monitoring_infos,
                    )
                )
            ),
            error=previous_result.error or last_result.error,
        )
    )
