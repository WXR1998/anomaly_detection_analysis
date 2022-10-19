import datetime
import json
import logging
import tqdm

from sam.base import messageAgent as ma, request, command, server, switch, vnf, sfc, link
from typing import Callable

from algo import AnomalyDetector
from model import TimeSeries
from netio import protocol

METRICS_LIST = {
    protocol.INSTANCE_TYPE_SWITCH: [
        'p4NFUsage',
        'tcamUsage'
    ],
    protocol.INSTANCE_TYPE_SERVER: [
        '_socketNum',
        '_coreUtilization',
        '_coreNUMADistribution',
        '_sizeOfTotalHugepages',
        '_dramCapacity',
        '_dramUsagePercentage',
        '_dramUsageAmount'
    ],
    protocol.INSTANCE_TYPE_VNFI: [
        'cpuCoreDistribution',
        'memNUMADistribution',
        'inputTrafficAmount',
        'inputPacketAmount',
        'outputTrafficAmount',
        'outputPacketAmount'
    ],
    protocol.INSTANCE_TYPE_LINK: [
        'utilization',
        'queueLatency'
    ]
}


class Core:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self,
                 detector: AnomalyDetector,
                 normal_window_length: int = 60,
                 debug: bool = True):
        self._detector = detector
        self._values = {}
        self._normal_window_length = normal_window_length
        self._debug = debug

        # zone, type, switch_id, server_id, link_id
        self._abnormal_result_handler = None
        # cmd_id, data
        self._dashboard_reply_handler = None
        self._topo_handler = None

        # {zone: {instance_type: {id: {metric: TimeSeries() }}}}
        self._instances = {
            ma.SIMULATOR_ZONE: {k: {} for k in METRICS_LIST.keys()},
            ma.TURBONET_ZONE: {k: {} for k in METRICS_LIST.keys()}
        }

        self._history_abnormal_result = {
            ma.SIMULATOR_ZONE: {k: {} for k in METRICS_LIST.keys()},
            ma.TURBONET_ZONE: {k: {} for k in METRICS_LIST.keys()}
        }

        self._topo = None

    @classmethod
    def singleton(cls):
        """
        除了第一次使用之外，都需调用此方法以获取单例
        @return:
        """
        if cls._instance is None:
            raise NameError('Core class not initialized.')
        return cls._instance

    def process_input_data(self, reply: request.Reply):
        logging.info(f'Received input data, request_id: {reply.requestID}')

        if not self._abnormal_result_handler:
            logging.error('Abnormal handler not registered. Results will not be sent.')

        data: dict = reply.attributes
        if self._topo is None:
            self._topo = {
                ma.SIMULATOR_ZONE: {},  # k-v: switch_id_src: set(switch_id_dst)
                ma.TURBONET_ZONE: {}
            }

            for zone in self._topo.keys():
                if zone not in data[protocol.INSTANCE_TYPE_LINK]:
                    continue
                links = data[protocol.INSTANCE_TYPE_LINK][zone].keys()
                for src, dst in links:
                    if src not in self._topo[zone]:
                        self._topo[zone][src] = set()
                    self._topo[zone][src].add(dst)

            self._topo_handler(self._topo)

        for instance_type in METRICS_LIST.keys():
            if instance_type not in data:
                continue
            instance_dict = data[instance_type]
            for zone in instance_dict.keys():
                zone_dict = instance_dict[zone]

                if self._debug:
                    progress = tqdm.tqdm(zone_dict.items())
                    progress.set_description(f'{instance_type}_{zone}')
                else:
                    progress = zone_dict.items()

                for id, d in progress:
                    obj = None
                    timestamp = 0
                    active = False
                    if instance_type == protocol.INSTANCE_TYPE_SWITCH:
                        obj: switch.Switch = d[protocol.ATTR_SWITCH]
                        active = d[protocol.ATTR_ACTIVE]
                    elif instance_type == protocol.INSTANCE_TYPE_SERVER:
                        obj: server.Server = d[protocol.ATTR_SERVER]
                        active = d[protocol.ATTR_ACTIVE]
                        timestamp = d[protocol.ATTR_TIMESTAMP]
                    elif instance_type == protocol.INSTANCE_TYPE_LINK:
                        obj: link.Link = d[protocol.ATTR_LINK]
                        active = d[protocol.ATTR_ACTIVE]
                    elif instance_type == protocol.INSTANCE_TYPE_SFCI:
                        obj: sfc.SFCI = d[protocol.ATTR_SFCI]
                        active = d[protocol.ATTR_ACTIVE]
                    elif instance_type == protocol.INSTANCE_TYPE_VNFI:
                        obj: vnf.VNFI = d[protocol.ATTR_VNFI]
                        active = d[protocol.ATTR_ACTIVE]
                    if not active:
                        continue

                    if id not in self._instances[zone][instance_type]:
                        self._instances[zone][instance_type][id] = {
                            metric: TimeSeries(normal_window_length=self._normal_window_length)
                            for metric in METRICS_LIST[instance_type]
                        }
                        self._history_abnormal_result[zone][instance_type][id] = {
                            metric: False
                            for metric in METRICS_LIST[instance_type]
                        }

                    for metric in METRICS_LIST[instance_type]:
                        value = obj.__getattribute__(metric)
                        self._instances[zone][instance_type][id][metric].add(value, timestamp)

                        if self._detector.tail_is_anomaly(self._instances[zone][instance_type][id][metric]):
                            inst = self._instances[zone][instance_type][id][metric]
                            values = inst.value()
                            mu = inst._mu
                            sigma = inst._sigma
                            upper = mu + sigma * self._detector._k
                            lower = mu - sigma * self._detector._k
                            # print(f'{instance_type} {id} {metric} [' + ', '.join([f'{_:.2f}' for _ in values]) + f']\t({lower:.2f}, {upper:.2f})')
                            if not self._history_abnormal_result[zone][instance_type][id][metric]:
                                if instance_type == protocol.INSTANCE_TYPE_SERVER:
                                    self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, server_id=id)
                                elif instance_type == protocol.INSTANCE_TYPE_LINK:
                                    self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, link_id=id)
                                elif instance_type == protocol.INSTANCE_TYPE_SWITCH:
                                    self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, switch_id=id)
                            else:   # Continuous abnormal suppression
                                pass

                            self._history_abnormal_result[zone][instance_type][id][metric] = True
                        else:
                            self._history_abnormal_result[zone][instance_type][id][metric] = False

    def process_dashboard_request(self, cmd: command.Command):
        logging.debug(f'Received dashboard request command, command_id: {cmd.cmdID}')

        if not self._dashboard_reply_handler:
            logging.error('Dashboard reply handler not registered. Results will not be sent.')

        attr: dict = cmd.attributes
        query_type = attr.get(protocol.ATTR_QUERY_TYPE)
        metric_name = attr.get(protocol.ATTR_METRIC_NAME, default=None)
        zone = attr.get(protocol.ATTR_ZONE)
        time_window = attr.get(protocol.ATTR_TIME_WINDOW, default=None)
        instance_type = attr.get(protocol.ATTR_INSTANCE_TYPE)
        instance_id_list = attr.get(protocol.ATTR_INSTANCE_ID_LIST, default=None)

        result = {}

        if query_type == protocol.QUERY_TYPE_HISTORY:
            data: dict = self._instances[zone][instance_type]
            for id in instance_id_list:
                result[id] = data[id][metric_name].raw_value(limit=time_window)

        elif query_type == protocol.QUERY_TYPE_ANOMALY:
            data: dict = self._instances[zone][instance_type]
            for id in instance_id_list:
                result[id] = self._detector.tail_is_anomaly(data[id][metric_name])

        elif query_type == protocol.QUERY_TYPE_FAILURE:
            data: dict = self._instances[zone][instance_type]
            for id in instance_id_list:
                result[id] = self._detector.tail_is_anomaly(data[id][metric_name])

        elif query_type == protocol.QUERY_TYPE_INSTANCE_ID:
            result = list(self._instances[zone][instance_type].keys())

        else:
            logging.error(f'Unknown query type: {query_type}')
            return

        reply_data = {
            protocol.ATTR_QUERY_TYPE: query_type,
            protocol.ATTR_INSTANCE_TYPE: instance_type,
            protocol.ATTR_VALUE: result
        }
        self._dashboard_reply_handler(cmd.cmdID, reply_data)

    def register_abnormal_result_handler(self, func: Callable):
        self._abnormal_result_handler = func

    def register_dashboard_reply_handler(self, func: Callable):
        self._dashboard_reply_handler = func

    def register_topo_handler(self, func: Callable):
        self._topo_handler = func
