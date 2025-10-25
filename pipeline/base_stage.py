# pipeline/base_stage.py
SENTINEL = None

class PipelineStage:
    """
    Template Method (very simple):
      - run(): fixed loop (get -> process -> emit)
      - process(item): override in subclasses
    """
    def __init__(self, in_q, out_q=None):
        self.in_q = in_q
        self.out_q = out_q

    def on_start(self): pass
    def on_finish(self): pass

    def process(self, item):
        raise NotImplementedError

    def run(self):
        print(self.__class__.__name__, "started.")
        self.on_start()
        while True:
            item = self.in_q.get()
            if item is None:
                break
            result = self.process(item)
            if result is not None and self.out_q is not None:
                self.out_q.put(result)
        if self.out_q is not None:
            self.out_q.put(SENTINEL)
        self.on_finish()
        print(self.__class__.__name__, "finished.")
