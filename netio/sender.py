import time
from concurrent.futures import ThreadPoolExecutor
import json

from abc import ABC, abstractmethod
from sam.base import messageAgent as ma
from . import protocol

class Sender(ABC):
    def __init__(self, dst_queue_name: str, interval: float=1):
        self._agent = ma.MessageAgent()
        self._dst_queue_name = dst_queue_name

        self._results = []
        self._interval = interval

    @abstractmethod
    def _send(self):
        pass

    def send(self):
        """
        主程序，建立一个线程用于监控待发送的指令，每秒轮询检测出的异常
        """

        def data_send_worker():
            while True:
                self._send()
                time.sleep(self._interval)

        def done_callback(worker):
            worker_exception = worker.exception()
            if worker_exception:
                print(worker_exception)

        ex = ThreadPoolExecutor(max_workers=1)
        ex.submit(data_send_worker).add_done_callback(done_callback)

class ResultSender(Sender):
    def __init__(self, dst_queue_name: str='result'):
        super().__init__(dst_queue_name)

    def add_anomaly_result(self, zone: str, type: str, switchID: int=0, serverID: int=0, linkID: int=0):
        """
        给某个组件添加一个anomaly，添加的组件的id不为0
        """

        assert zone in [ma.TURBONET_ZONE,  ma.SIMULATOR_ZONE]
        assert type in [protocol.ABNORMAL, protocol.FAILURE]
        self._results.append((zone, type, switchID, serverID, linkID))

    def _send(self):
        data = {
            protocol.ALL_ZONE_DETECTION_DICT: {
                ma.TURBONET_ZONE: {
                    protocol.FAILURE: {
                        protocol.SWITCH_ID_LIST: [],
                        protocol.SERVER_ID_LIST: [],
                        protocol.LINK_ID_LIST: []
                    },
                    protocol.ABNORMAL: {
                        protocol.SWITCH_ID_LIST: [],
                        protocol.SERVER_ID_LIST: [],
                        protocol.LINK_ID_LIST: []
                    },
                },
                ma.SIMULATOR_ZONE: {
                    protocol.FAILURE: {
                        protocol.SWITCH_ID_LIST: [],
                        protocol.SERVER_ID_LIST: [],
                        protocol.LINK_ID_LIST: []
                    },
                    protocol.ABNORMAL: {
                        protocol.SWITCH_ID_LIST: [],
                        protocol.SERVER_ID_LIST: [],
                        protocol.LINK_ID_LIST: []
                    },
                }
            }
        }

        if len(self._results) > 0:
            while len(self._results) > 0:
                zone, type, switchID, serverID, linkID = self._results[0]
                self._results.pop(0)

                if switchID > 0:
                    data[protocol.ALL_ZONE_DETECTION_DICT][zone][type][protocol.SWITCH_ID_LIST].append(switchID)
                if serverID > 0:
                    data[protocol.ALL_ZONE_DETECTION_DICT][zone][type][protocol.SWITCH_ID_LIST].append(serverID)
                if linkID > 0:
                    data[protocol.ALL_ZONE_DETECTION_DICT][zone][type][protocol.SWITCH_ID_LIST].append(linkID)

            self._agent.sendMsg(self._dst_queue_name, ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD, json.dumps(data)))

