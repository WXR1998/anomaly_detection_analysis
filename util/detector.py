import logging

from algo import AnomalyDetection
from netio import ResultSender, DataReceiver
from model import TimeSeries

from sam.base import messageAgent as ma
from sam.base import switch, server, sfc, vnf, link
from netio import protocol

INSTANCE_TYPE_SWITCH = protocol.REQUEST_INSTANCE_TYPE_SWITCH
INSTANCE_TYPE_SERVER = protocol.REQUEST_INSTANCE_TYPE_SERVER
INSTANCE_TYPE_LINK = protocol.REQUEST_INSTANCE_TYPE_LINK
INSTANCE_TYPE_VNFI = 'vnfi'
INSTANCE_TYPE_SFCI = 'sfci'

METRICS_LIST = {
    INSTANCE_TYPE_SWITCH: ['tcamUsage', 'p4NFUsage'],
    INSTANCE_TYPE_SERVER: ['_socketNum', '_numaNum', '_coreUtilization', '_hugepagesTotal', '_hugepagesFree', '_hugepageSize'],
    INSTANCE_TYPE_VNFI: ['inputTrafficAmount', 'inputPacketAmount', 'outputTrafficAmount', 'outputPacketAmount'],
    INSTANCE_TYPE_LINK: ['utilization', 'queueLatency']
}

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

        self._instances = {
            INSTANCE_TYPE_SWITCH: {},
            INSTANCE_TYPE_SERVER: {},
            INSTANCE_TYPE_LINK: {},
            INSTANCE_TYPE_VNFI: {},
            INSTANCE_TYPE_SFCI: {},
        }

    def process_input_data(self, data: object):
        logging.info(f'Received data: {type(data)}')

        def append_new_data(id: str, instance_type: str, data: object=data):
            if id not in self._instances[instance_type]:
                instance_info = {}
                for metric in METRICS_LIST[instance_type]:
                    instance_info[metric] = TimeSeries(normal_window_length=self._normal_window_length)
                self._instances[instance_type][id] = instance_info

            for metric in METRICS_LIST[instance_type]:
                self._instances[instance_type][id][metric].add(data.__getattribute__(metric))
                if self._detector.tail_is_anomaly(self._instances[instance_type][id][metric]) or True:
                    if instance_type == INSTANCE_TYPE_SERVER:
                        self._sender.add_anomaly_result(zone=ma.TURBONET_ZONE, type=protocol.FAILURE, serverID=id)
                    elif instance_type == INSTANCE_TYPE_SWITCH:
                        self._sender.add_anomaly_result(zone=ma.TURBONET_ZONE, type=protocol.FAILURE, switchID=id)
                    elif instance_type == INSTANCE_TYPE_LINK:
                        self._sender.add_anomaly_result(zone=ma.TURBONET_ZONE, type=protocol.FAILURE, linkID=id)

        data_type = type(data)
        if data_type == switch.Switch:
            id = str(data.switchID)
            append_new_data(id, INSTANCE_TYPE_SWITCH)

        elif data_type == server.Server:
            id = str(data._serverID)
            append_new_data(id, INSTANCE_TYPE_SERVER)

        elif data_type == sfc.SFCI:
            pass

        elif data_type == vnf.VNFI:
            id = str(data.vnfiID)
            if data.vnfiStatus is not None:
                status = data.vnfiStatus
                append_new_data(id, INSTANCE_TYPE_VNFI, data=status)

        elif data_type == link.Link:
            id = f'{data.srcID}_{data.dstID}'
            append_new_data(id, INSTANCE_TYPE_LINK)

    def main(self):
        self._sender.send()
        self._receiver.receive()

    def query_history_value(self, instance_type: str, instance_id: str):
        types = [INSTANCE_TYPE_SWITCH, INSTANCE_TYPE_LINK, INSTANCE_TYPE_SERVER]
        if instance_type not in types or instance_id not in self._instances[instance_type]:
            return None, None
        ts = self._instances[instance_type][instance_id]
        return ts.timestamp(), ts.value()

    def query_anomaly_record(self, instance_type: str, instance_id: str):
        return None, None