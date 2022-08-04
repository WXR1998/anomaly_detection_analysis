import random
import logging

from algo import AnomalyDetection
from netio import ResultSender, DataReceiver
from model import TimeSeries

from sam.base import messageAgent as ma
from netio import protocol

class AnomalyDetector:
    def __init__(self,
                 detector: AnomalyDetection,
                 src_queue_name: str='data',
                 dst_queue_name: str='result',
                 normal_window_length: int=60):
        self._detector = detector
        self._values = {}
        self._receiver = DataReceiver(src_queue_name=src_queue_name,
                                      process_func=self.process_input_data)
        self._sender = ResultSender(dst_queue_name=dst_queue_name)
        self._normal_window_length = normal_window_length

    def process_input_data(self, data: tuple):
        metric, value, last, current = data

        if metric not in self._values:
            self._values[metric] = TimeSeries(normal_window_length=self._normal_window_length)
        self._values[metric].add(value)

        logging.info(f'Received data: {metric} {value:.3f}')
        if self._detector.tail_is_anomaly(self._values[metric]):
            self._sender.add_anomaly_result(zone=ma.TURBONET_ZONE, type=protocol.FAILURE,
                                            serverID=random.randint(1, 10))

    def main(self):
        self._sender.send()
        self._receiver.receive()