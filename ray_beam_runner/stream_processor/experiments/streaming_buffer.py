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
from queue import Queue

from ray_beam_runner.stream_processor.utils import KeyedValue


class SDKWorkerBuffer(object):

    def __init__(self):
        self.stage_actor = None
        self.stage_actor_listener = None
        self.queue = Queue()

    def register_listener(self, stage_actor: 'StageActor'):
        self.stage_actor = stage_actor

    def insert_wm(self, keyed_wm: KeyedValue):
        self.queue.put((None, keyed_wm))
        return self.checkpoint()

    def put(self, item: KeyedValue):
        self.queue.put((item, None))
        return self.checkpoint()

    def get(self):
        # TODO Convert this to lease system
        return self.queue.get()

    def checkpoint(self):
        # TODO Must checkpoint the Queue when a watermark change happens
        return True
