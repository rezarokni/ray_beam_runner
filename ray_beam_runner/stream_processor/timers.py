from typing import Iterator, List, Tuple

from sortedcontainers import SortedDict


class TimerMgr(object):
    def __init__(self):
        self.timers: SortedDict[int, List[str]] = SortedDict()

        # TODO add index so that Timer update and Timer delete do not require list search
        # self.timers_index: SortedDict[str, int] = SortedDict()

    def add_timers(self, timer: Tuple[str, int]):
        timestamp = timer[1]
        if timestamp not in self.timers:
            self.timers.update(timer[1], [timer[0]])
        else:
            self.timers.get(timestamp).append(timer[0])

    def fire_timers(self, wm: int):
        timer_bundle = self.timers.islice(stop=wm)
        self.process_timers(timer_bundle)
        self.timers.__delitem__()
        return timer_bundle

    def process_timers(self, timers: Iterator[str]):
        # Send to beam sdk
        pass
