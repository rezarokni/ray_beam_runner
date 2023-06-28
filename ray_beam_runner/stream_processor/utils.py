# Send keys off to be processed, we will have each key == a window in time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Dict, Tuple
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.worker import bundle_processor


class KeyedValue(object) :

    def __init__(self, key: Any, value: Any):
        self.key = key
        self.value = value


class StageWorkerStatus(Enum):
    STOPPED = 1
    PROCESSING = 2


@dataclass
class StageDependencies:
    input: List[Tuple[str, str]]
    side_input: List[Tuple[str, str]]

def assemble_dag(stages: List[translations.Stage]) -> Dict[str, StageDependencies]:
    """Assembles a DAG of stages and their dependencies.

    It performs this by iterating through the stages and their transforms and
    looking for DATA_INPUT and DATA_OUTPUT transforms - these transforms consume
    PCollections from other stages (or the IMPULSE buffer).
    From the DATA_OUTPUT transforms, we can determine the producer of each PCollection,
    and from there, the producer stage.
    """
    stages_by_name = {}
    pcolls_by_producer_stage = defaultdict(list)
    dag: Dict[str, StageDependencies] = {}

    for s in stages:
        stage_name = s.name
        stages_by_name[s.name] = s
        dag[stage_name] = StageDependencies([], [])

        for t in s.transforms:
            if t.spec.urn == bundle_processor.DATA_INPUT_URN:
                buffer_id = t.spec.payload
                if buffer_id == translations.IMPULSE_BUFFER:
                    pcoll_name = t.unique_name
                    producer_stages = []
                else:
                    _, pcoll_name = translations.split_buffer_id(buffer_id)
                    producer_stages = pcolls_by_producer_stage[pcoll_name]
                for producer_name in producer_stages:
                    dag[stage_name].input.append((producer_name, pcoll_name))

            if t.spec.urn == bundle_processor.DATA_OUTPUT_URN:
                buffer_id = t.spec.payload
                _, pcoll_name = translations.split_buffer_id(buffer_id)
                pcolls_by_producer_stage[pcoll_name].append(stage_name)

            for pcoll_name in s.side_inputs():
                producer_stages = pcolls_by_producer_stage[pcoll_name]
                for producer_name in producer_stages:
                    dag[producer_name].side_input.append((producer_name, pcoll_name))

    return dag
