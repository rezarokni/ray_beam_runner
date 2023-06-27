class WaterMarkMgr(object):

    def __init__(self, input_wm : int = 0 ):
        self.wm_tracker = dict()
        self.input_wm = input_wm
        self.min_wm = 0

    def update_input_wm(self, wm: int):
        if self.min_wm < wm:
            self.min_wm = wm
            self.broadcast()

    def update(self, key: str, wm: int):

        if self.wm_tracker[key] < wm:
            self.wm_tracker[key] = wm
            self.checkpoint()

        return True

    def checkpoint(self):
        pass

    def broadcast(self):
        pass

    class WaterMarkTracker:

        def __init__(self):
            self.key_range = None
            self.wm = None