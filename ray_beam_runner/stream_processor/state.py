import contextlib
import collections
import logging
from typing import List

import ray
from apache_beam.portability.api.org.apache.beam.model import fn_execution

from ray import ObjectRef

from typing import Optional,Tuple, Iterator, TypeVar
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import sdk_worker

_LOGGER = logging.getLogger(__name__)


T = TypeVar("T")

class RayFuture(sdk_worker._Future[T]):
  """Wraps a ray ObjectRef in a beam sdk_worker._Future"""

  def __init__(self, object_ref):
    # type: (ObjectRef[T]) -> None
    self._object_ref: ObjectRef[T] = object_ref

  def wait(self, timeout=None):
    # type: (Optional[float]) -> bool
    try:
      # TODO: Is ray.get slower than ray.wait if we don't need the return value?
      ray.get(self._object_ref, timeout=timeout)
      #
      return True
    except ray.GetTimeoutError:
      return False

  def get(self, timeout=None):
    # type: (Optional[float]) -> T
    return ray.get(self._object_ref, timeout=timeout)

  def set(self, _value):
    # type: (T) -> sdk_worker._Future[T]
    raise NotImplementedError()


@ray.remote
class _ActorStateManager:
  def __init__(self):
    self._data = collections.defaultdict(lambda: [])

  def get_raw(
      self,
      state_key: str,
      continuation_token: Optional[bytes] = None,
  ) -> Tuple[bytes, Optional[bytes]]:
    if continuation_token:
      continuation_token = int(continuation_token)
    else:
      continuation_token = 0

    full_state = self._data[state_key]
    if len(full_state) == continuation_token:
      return b"", None

    if continuation_token + 1 == len(full_state):
      next_cont_token = None
    else:
      next_cont_token = str(continuation_token + 1).encode("utf8")

    return full_state[continuation_token], next_cont_token

  def append_raw(self, state_key: str, data: bytes):
    self._data[state_key].append(data)

  def clear(self, state_key: str):
    self._data[state_key] = []


class RayStateManager(sdk_worker.StateHandler):
  def __init__(self, state_actor: Optional[_ActorStateManager] = None):
    self._state_actor = state_actor or _ActorStateManager.remote()
    self._instruction_id: Optional[str] = None

  @staticmethod
  def _to_key(state_key: beam_fn_api_pb2.StateKey):
    return state_key.SerializeToString()

  def get_raw(
      self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      continuation_token=None,  # type: Optional[bytes]
  ) -> Tuple[bytes, Optional[bytes]]:
    return ray.get(
        self._state_actor.get_raw.remote(
            RayStateManager._to_key(state_key),
            continuation_token,
        )
    )

  def append_raw(self, state_key: beam_fn_api_pb2.StateKey, data: bytes) -> RayFuture:
    return RayFuture(
        self._state_actor.append_raw.remote(
            RayStateManager._to_key(state_key), data
        )
    )

  def clear(self, state_key: beam_fn_api_pb2.StateKey) -> RayFuture:
    assert self._instruction_id is not None
    return RayFuture(
        self._state_actor.clear.remote(RayStateManager._to_key(state_key))
    )

  @contextlib.contextmanager
  def process_instruction_id(self, bundle_id: str) -> Iterator[None]:
    # Instruction id is not being used right now,
    # we only assert that it has been set before accessing state.
    self._instruction_id = bundle_id
    yield
    self._instruction_id = None

  def done(self):
    pass


class PcollectionBufferManager:
    def __init__(self):
        self.buffers = collections.defaultdict(list)

    def put(self, pcoll, data_refs: List[ray.ObjectRef]):
        self.buffers[pcoll].extend(data_refs)

    def get(self, pcoll) -> List[ray.ObjectRef]:
        return self.buffers[pcoll]

    def clear(self, pcoll):
        self.buffers[pcoll].clear()
