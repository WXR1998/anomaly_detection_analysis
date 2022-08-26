from netio import Receiver


class MockResultReceiver(Receiver):
    def __init__(self, src_queue_name: str='result', process_func=None):
        super().__init__(src_queue_name, process_func)