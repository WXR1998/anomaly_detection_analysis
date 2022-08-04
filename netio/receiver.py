from abc import abstractmethod, ABC
from concurrent.futures import ThreadPoolExecutor
import json

from sam.base import messageAgent as ma
from . import protocol

class Receiver(ABC):
    def __init__(self, src_queue_name: str, process_func=None):
        """
        绑定一个处理函数到queue的数据上。每来一个新的数据，都会调用process_func用于处理之。
        调用receive()会起一个新线程用于处理数据。
        """
        self._agent = ma.MessageAgent()
        self._src_queue_name = src_queue_name
        self._process_func = process_func

    def receive(self):
        self._agent.startRecvMsg(self._src_queue_name)

        def data_receive_worker():
            while True:
                msg = self._agent.getMsg(self._src_queue_name)
                if not msg.getMessageType() is None:
                    if self._process_func is not None:
                        self._process_func(self.parse_data(msg.getbody()))

        def done_callback(worker):
            worker_exception = worker.exception()
            if worker_exception:
                print(worker_exception)

        ex = ThreadPoolExecutor(max_workers=1)
        ex.submit(data_receive_worker).add_done_callback(done_callback)

    @staticmethod
    @abstractmethod
    def parse_data(data_str: str):
        return data_str

class DataReceiver(Receiver):
    def __init__(self, src_queue_name: str='data', process_func=None):
        super().__init__(src_queue_name, process_func)

    @staticmethod
    def parse_data(data_str: str):
        """
        处理输入数据，将其分拆成 metric, value, last, current 四个值
        """
        obj = json.loads(data_str)

        metric_name = obj[protocol.METRIC]
        value = obj[protocol.VALUE]
        last = obj[protocol.LAST]
        current = obj[protocol.CURRENT]

        return metric_name, value, last, current
