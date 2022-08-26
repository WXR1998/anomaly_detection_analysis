import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor

from sam.base import messageAgent as ma

from util import AnomalyDetector
from . import protocol, FrontendReplySender


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
                        self._process_func(msg.getbody())

        def done_callback(worker):
            worker_exception = worker.exception()
            if worker_exception:
                print(worker_exception)

        ex = ThreadPoolExecutor(max_workers=1)
        ex.submit(data_receive_worker).add_done_callback(done_callback)

class DataReceiver(Receiver):
    def __init__(self, src_queue_name: str='data', process_func=None):
        super().__init__(src_queue_name, process_func)

class FrontendRequestReceiver(Receiver):
    def __init__(self, src_queue_name: str='frontend_request',
                 anomaly_detector: AnomalyDetector=None,
                 sender: FrontendReplySender=None):
        """
        接收前端发来的查询请求，返回对应的结果
        """

        super().__init__(src_queue_name, self.request_handler)
        assert anomaly_detector is not None
        assert sender is not None

        self._anomaly_detector = anomaly_detector
        self._sender = sender

    def request_handler(self, data: dict):
        uuid = data[protocol.UUID]
        query_type = data[protocol.REPLY_QUERY_TYPE]
        instance_type = data[protocol.REPLY_INSTANCE_TYPE]
        instance_id = data[protocol.REPLY_INSTANCE_ID]

        result = None, None
        if query_type == protocol.REQUEST_QUERY_TYPE_HISTORY_VALUE:
            result = self._anomaly_detector.query_history_value(instance_type, instance_id)
        elif query_type == protocol.REQUEST_QUERY_TYPE_ANOMALY_RECORD:
            result = self._anomaly_detector.query_anomaly_record(instance_type, instance_id)
        else:
            logging.warning(f'uuid: {uuid} 接收到 query_type 为 {query_type} 的请求，非法')
        timestamps, values = result

        if timestamps is None:
            logging.warning(f'uuid: {uuid} 请求返回空结果')

        self._sender.add_reply(uuid, timestamps, values)

