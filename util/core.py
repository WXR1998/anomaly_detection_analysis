import datetime
import logging
import copy
import numpy as np
import tqdm

from sam.base import request, command, server, switch, vnf, sfc, link
from typing import Callable, Optional

from model import TimeSeries
from netio import protocol

class Core:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def singleton(cls):
        """
        除了第一次使用之外，都需调用此方法以获取单例
        @return:
        """
        if cls._instance is None:
            raise NameError('尚未初始化Core')
        return cls._instance

    def __init__(self,
                 k: float = 5,
                 normal_window_length: int = 60,
                 abnormal_window_length: int = 2,
                 cooldown: int = 30,
                 debug: bool = True):
        self._values = {}
        self._normal_window_length = normal_window_length
        self._abnormal_window_length = abnormal_window_length
        self._debug = debug
        self._k = k
        self._cpu_thres = 0.2
        # 告警冷却时间
        self._cooldown = cooldown
        # link.utilization的异常判定阈值
        self._link_util_thres = 0.7

        # zone, type, switch_id, server_id, link_id
        self._abnormal_result_handler = None
        # cmd_id, data
        self._dashboard_reply_handler = None
        self._topo_handler = None

        """
        {
            zone: {
                instance_type: {
                    id: {
                        history_value: [
                            (timestamp, object)
                        ], 
                        metrics: {
                            metric_name: TimeSeries
                        },
                        abnormal_state: bool,
                        failure_state: bool
                    }
                }
            }
        }
        """
        self._instances = {
            zone: {k: {} for k in protocol.INSTANCE_TYPES}
            for zone in protocol.ZONES
        }

        self._topo = None

    def _build_topology(self, data: dict):
        """
        从发来的数据中构建网络拓扑，并调用topo_handler
        Args:
            data: 发来的数据包
        """
        assert self._topo is None

        self._topo = {zone: {} for zone in protocol.ZONES}
        for zone in self._topo.keys():
            if zone not in data[protocol.INSTANCE_TYPE_LINK]:
                continue
            link_ids = data[protocol.INSTANCE_TYPE_LINK][zone].keys()
            for src, dst in link_ids:
                if src not in self._topo[zone]:
                    self._topo[zone][src] = set()
                self._topo[zone][src].add(dst)

        self._topo_handler(self._topo)

    def reset_ksigma(self):
        """
        根据收到的重置命令，重置ksigma算法的历史数据
        """
        logging.warning('重置k-sigma算法的历史数据')
        for zone in protocol.ZONES:
            for instance_type in protocol.INSTANCE_TYPES:
                d = self._instances[zone][instance_type]
                for idx in d.keys():
                    for metric in d[idx][protocol.ATTR_METRICS].keys():
                        d[idx][protocol.ATTR_METRICS][metric].reset()

    def _new_timeseries(self):
        return copy.deepcopy(
            TimeSeries(normal_window_length=self._normal_window_length,
                       abnormal_window_length=self._abnormal_window_length,
                       k=self._k)
        )

    @staticmethod
    def _new_instance():
        return copy.deepcopy({
            protocol.ATTR_HISTORY_VALUE: [],
            protocol.ATTR_METRICS: {},
            protocol.ATTR_ABNORMAL_STATE: False,
            protocol.ATTR_FAILURE_STATE: False,
            protocol.ATTR_LAST_ABNORMAL: 0,
            protocol.ATTR_LAST_FAILURE: 0
        })

    def process_input_data(self, reply: request.Reply):
        logging.info(f'收到从measurer发来的数据')
        timestamp = int(datetime.datetime.now().timestamp())

        if not self._abnormal_result_handler:
            logging.error('尚未注册异常报警函数，结果将不会发送。')

        data: dict = reply.attributes
        if self._topo is None:
            self._build_topology(data)

        for instance_type in protocol.INSTANCE_TYPES:
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

                for idx, d in progress:
                    obj = None
                    active = d[protocol.ATTR_ACTIVE]
                    if instance_type == protocol.INSTANCE_TYPE_SWITCH:
                        obj: Optional[switch.Switch]    = d[protocol.ATTR_SWITCH]
                    elif instance_type == protocol.INSTANCE_TYPE_SERVER:
                        obj: Optional[server.Server]    = d[protocol.ATTR_SERVER]
                    elif instance_type == protocol.INSTANCE_TYPE_LINK:
                        obj: Optional[link.Link]        = d[protocol.ATTR_LINK]
                    elif instance_type == protocol.INSTANCE_TYPE_SFCI:
                        obj: Optional[sfc.SFCI]         = d[protocol.ATTR_SFCI]
                    elif instance_type == protocol.INSTANCE_TYPE_VNFI:
                        obj: Optional[vnf.VNFI]         = d[protocol.ATTR_VNFI]

                    if idx not in self._instances[zone][instance_type]:
                        self._instances[zone][instance_type][idx] = self._new_instance()

                    self._instances[zone][instance_type][idx][protocol.ATTR_HISTORY_VALUE].append((timestamp, obj))
                    self._instances[zone][instance_type][idx][protocol.ATTR_FAILURE_STATE] = not active

                    if not active:  # 不是active，则证明其已经属于failure，不属于abnormal
                        last_failure = self._instances[zone][instance_type][idx][protocol.ATTR_LAST_FAILURE]
                        if datetime.datetime.now().timestamp() - last_failure >= self._cooldown:
                            if instance_type == protocol.INSTANCE_TYPE_SERVER:
                                self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, server_id=idx)
                            if instance_type == protocol.INSTANCE_TYPE_SWITCH:
                                self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, switch_id=idx)
                            if instance_type == protocol.INSTANCE_TYPE_LINK:
                                self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, link_id=idx)
                            self._instances[zone][instance_type][idx][protocol.ATTR_LAST_FAILURE] = \
                                datetime.datetime.now().timestamp()
                        continue

                    instance_dict = self._instances[zone][instance_type][idx][protocol.ATTR_METRICS]

                    if instance_type == protocol.INSTANCE_TYPE_SERVER:  # 服务器，需要对其CPU和内存施行异常检测
                        if protocol.ATTR_SERVER_CPU_UTILIZATION not in instance_dict:
                            instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION] = self._new_timeseries()
                            instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION] = self._new_timeseries()

                        if protocol.ATTR_SERVER_CPU_UTILIZATION not in instance_dict or \
                                protocol.ATTR_SERVER_MEMORY_UTILIZATION not in instance_dict:
                            continue

                        cpu_util_value = float(np.nanmean(obj.getCpuUtil()))
                        instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION].add(cpu_util_value)
                        mem_util_value = obj.getDRAMUsagePercentage()
                        instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION].add(mem_util_value)

                        abnormal = \
                            cpu_util_value > self._cpu_thres and \
                            (instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION].is_abnormal() or
                            instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION].is_abnormal())
                        self._instances[zone][instance_type][idx][protocol.ATTR_ABNORMAL_STATE] = abnormal

                        last_abnormal = self._instances[zone][instance_type][idx][protocol.ATTR_LAST_ABNORMAL]
                        if abnormal and datetime.datetime.now().timestamp() - last_abnormal >= self._cooldown:
                            print(f'server: {idx}\nCPU: {instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION].value()}\nmemory:{instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION].value()}')
                            self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, server_id=idx)
                            self._instances[zone][instance_type][idx][protocol.ATTR_LAST_ABNORMAL] = \
                                datetime.datetime.now().timestamp()

                    elif instance_type == protocol.INSTANCE_TYPE_LINK:  # 链路，需要对其SYN包等统计信息施行异常检测
                        if protocol.ATTR_LINK_SYN_RATIO not in instance_dict:
                            instance_dict[protocol.ATTR_LINK_SYN_RATIO] = self._new_timeseries()
                            instance_dict[protocol.ATTR_LINK_DNS_RATIO] = self._new_timeseries()

                        nsh_num_value = obj.NSH_num
                        syn_num_value = obj.SYN_num
                        dns_num_value = obj.DNS_num
                        link_util_value = obj.utilization
                        total_num_value = nsh_num_value + syn_num_value + dns_num_value

                        if protocol.ATTR_LINK_SYN_RATIO not in instance_dict or \
                                protocol.ATTR_LINK_DNS_RATIO not in instance_dict:
                            continue

                        syn_ratio_value = syn_num_value / total_num_value if total_num_value > 0 else 0
                        instance_dict[protocol.ATTR_LINK_SYN_RATIO].add(syn_ratio_value)
                        dns_ratio_value = dns_num_value / total_num_value if total_num_value > 0 else 0
                        instance_dict[protocol.ATTR_LINK_DNS_RATIO].add(dns_ratio_value)

                        # util大于阈值，且DNS包或者SYN包的比例有大的变化
                        abnormal = \
                            link_util_value > self._link_util_thres and \
                            (instance_dict[protocol.ATTR_LINK_SYN_RATIO].is_abnormal() or
                             instance_dict[protocol.ATTR_LINK_DNS_RATIO].is_abnormal())
                        self._instances[zone][instance_type][idx][protocol.ATTR_ABNORMAL_STATE] = abnormal

                        last_abnormal = self._instances[zone][instance_type][idx][protocol.ATTR_LAST_ABNORMAL]
                        if abnormal and datetime.datetime.now().timestamp() - last_abnormal >= self._cooldown:
                            self._abnormal_result_handler(zone, protocol.ATTR_ABNORMAL, link_id=idx)
                            self._instances[zone][instance_type][idx][protocol.ATTR_LAST_ABNORMAL] = \
                                datetime.datetime.now().timestamp()

    def process_dashboard_request(self, cmd: command.Command):
        logging.info(f'收到前台查询请求，command_id: {cmd.cmdID}')

        if not self._dashboard_reply_handler:
            logging.error('尚未注册前台处理函数。')

        attr: dict = cmd.attributes
        query_type = attr.get(protocol.ATTR_QUERY_TYPE)
        metric_name = attr.get(protocol.ATTR_METRIC_NAME, default=None)
        zone = attr.get(protocol.ATTR_ZONE)
        time_window = attr.get(protocol.ATTR_TIME_WINDOW, default=None)
        instance_type = attr.get(protocol.ATTR_INSTANCE_TYPE)
        instance_id_list = attr.get(protocol.ATTR_INSTANCE_ID_LIST, default=None)

        result = {}

        data: dict = self._instances[zone][instance_type]
        if query_type == protocol.QUERY_TYPE_HISTORY:
            for idx in instance_id_list:
                history = data[idx][protocol.ATTR_HISTORY_VALUE]
                ts = [ts for ts, obj in history]
                value = [obj for ts, obj in history]
                result[idx] = {
                    protocol.ATTR_TIMESTAMP: ts,
                    protocol.ATTR_VALUE: value
                }

        elif query_type == protocol.QUERY_TYPE_ANOMALY:
            for idx in instance_id_list:
                ts = data[idx][protocol.ATTR_HISTORY_VALUE][-1][0]
                value = data[idx][protocol.ATTR_ABNORMAL_STATE]
                result[idx] = {
                    protocol.ATTR_TIMESTAMP: [ts],
                    protocol.ATTR_VALUE: [value]
                }

        elif query_type == protocol.QUERY_TYPE_FAILURE:
            for idx in instance_id_list:
                ts = data[idx][protocol.ATTR_HISTORY_VALUE][-1][0]
                value = data[idx][protocol.ATTR_FAILURE_STATE]
                result[idx] = {
                    protocol.ATTR_TIMESTAMP: [ts],
                    protocol.ATTR_VALUE: [value]
                }

        elif query_type == protocol.QUERY_TYPE_INSTANCE_ID:
            result = list(self._instances[zone][instance_type].keys())

        else:
            logging.error(f'未知的查询类型： {query_type}')
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
