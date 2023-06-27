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
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Tuple, Dict

import ray

from ray_beam_runner.stream_processor.timers import TimerMgr
from ray_beam_runner.stream_processor.experiments.streaming_buffer import SDKWorkerBuffer
from ray_beam_runner.stream_processor.utils import KeyedValue, StageWorkerStatus


class StageLifeCycle(Enum):
    STARTING = 0
    RUNNING = 1


def get_impulse() -> KeyedValue:
    return KeyedValue(uuid.uuid4(), None)


class HealthChecker(object):
    def __init__(self, sdk):
        self.sdk = sdk


class SDK(ABC):
    @abstractmethod
    def do(self, bundle: KeyedValue) -> KeyedValue:
        pass


@ray.remote
class SDKWorkerActor(object):

    def __init__(self, no_parents: bool, sdk: SDK):
        self.sdk = sdk
        self.no_parents: bool = no_parents
        self.health_checkers = list()
        self.input_buffer = SDKWorkerBuffer()
        self.output_buffers: Dict[str, SDKWorkerBuffer] = dict()
        self.status = StageWorkerStatus.STOPPED
        self.timerMgr = TimerMgr()

    def start_sdks(self):
        self.health_checkers.append(HealthChecker(123))

    def update_input_buffer(self, item: KeyedValue):
        self.output_buffer.put(item)

    def do(self, bundle: KeyedValue):
        bundle_response = self.sdk.do(bundle=bundle)
        bundle_id = bundle_response.key
        bundle_data = bundle_response.value

        for b in bundle_data:
            if b.key is not None:
                # TODO Process Result
                print(b.value)
                pass
            else:
                print("watermark observed")
                # self.wm_manager.update(b.value)
        #       Store Timers
        #       Store State Data
        #       Send Key to forward buffers

    def lease_work(self) -> Tuple[KeyedValue, List]:
        return self.input_buffer.get()

    def start(self):
        print('starting worker process')
        self._start()

    def _start(self):
        self.status = StageWorkerStatus.PROCESSING
        while self.status is StageWorkerStatus.PROCESSING:
            if self.no_parents:
                work = get_impulse()
            else:
                work = self.lease_work()
            self.do(work)


@ray.remote(scheduling_strategy="SPREAD")
class StageActor(object):

    def __init__(self, sdk: SDK):
        self.sdk = sdk
        self.num_stage_workers = 2
        self.parents: ['StageActor'] = list()
        self.children: ['StageActor'] = list()
        self.workers: dict[str, SDKWorkerActor] = dict()
        self.running_state = StageLifeCycle.STARTING

    def pre_check_in_starting(self):
        if self.running_state != StageLifeCycle.STARTING:
            raise Exception('Action only permitted during INIT')

    def find_key_channel(self, key: str):
        worker_id = hash(key) % self.num_stage_workers
        if worker_id in self.workers:
            return self.workers[worker_id]
        else:
            raise Exception('Worker does not exist')

    def init_workers(self):
        self.pre_check_in_starting()
        for worker_id in range(self.num_stage_workers):
            self.workers[worker_id] = SDKWorkerActor.remote(no_parents=(not self.parents), sdk=self.sdk)
            self.workers[worker_id].start.remote()

    def start(self):
        print("Starting Stage")
        for k, v in self.workers.items():
            v.start.remote()
            print("Started worker " + str(k))

        self.running_state = StageLifeCycle.RUNNING

    def register_parents(self, parents: List['StageActor']):
        self.pre_check_in_starting()
        self.parents.extend(parents)
        for p in self.parents:
            p.register_children(self)

    def register_children(self, children: List['StageActor']):
        self.pre_check_in_starting()
        self.children.extend(children)
