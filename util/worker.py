import time
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue
import copy
import datetime
import numpy as np
import logging

import sam.base.link
from sam.base import messageAgent as ma, command

from model import TimeSeries
from netio import protocol
from util import threading


class Worker(ABC):
    def __init__(
            self,
            k: float,
            data_queue: Queue,  # 主进程发来的原始数据
            anom_queue: Queue,  # 应该报告异常的元素结果
            cmd_queue: Queue,  # 主进程发来的命令
            res_queue: Queue,  # 应该发给主进程的前端查询结果
            history_len_limit: int,
            cooldown: int,
            normal_window_length: int,
            abnormal_window_length: int,
            debug: bool,
            name: str,
    ):
        """
        data_queue: {
            protocol.ATTR_TIMESTAMP: xxx,
            protocol.ATTR_INSTANCE_TYPE: xxx,
            protocol.ATTR_ZONE: xxx,
            protocol.ATTR_ACTIVE: xxx,
            protocol.ATTR_VALUE: xxx
            protocol.ATTR_ID: xxx
        }
        """

        self._data_queue = data_queue
        self._anom_queue = anom_queue
        self._cmd_queue = cmd_queue
        self._res_queue = res_queue

        self._name = name
        self._history_len_limit = history_len_limit
        self._cooldown = cooldown
        self._normal_window_length = normal_window_length
        self._abnormal_window_length = abnormal_window_length
        self._k = k
        self._debug = debug
        self._last_reset = datetime.datetime.now().timestamp()

        self._instances = {}
        self._link_util_thres = 0.5
        self._link_packet_num_thres = 10000

        self._count = 0

    @staticmethod
    def _new_instance():
        return copy.deepcopy({
            protocol.ATTR_HISTORY_VALUE: None,
            protocol.ATTR_METRICS: {},
            protocol.ATTR_ABNORMAL_STATE: 0,  # 最后一次abnormal的时间，注意即使处在报警冷却过程中，该值仍然需要更新
            protocol.ATTR_FAILURE_STATE: False,
            protocol.ATTR_LAST_ABNORMAL: 0,
            protocol.ATTR_LAST_FAILURE: 0
        })

    def _add_anomaly_report(
            self,
            zone: str,
            anom_type: str,
            switch_id: str = None,
            server_id: str = None,
            link_id: tuple = None
    ):
        """
        给某个组件添加一个anomaly，不能三个全为None
        """

        assert zone in [ma.TURBONET_ZONE, ma.SIMULATOR_ZONE]
        assert anom_type in [protocol.ATTR_ABNORMAL, protocol.ATTR_FAILURE]
        self._anom_queue.put((zone, anom_type, switch_id, server_id, link_id))

    def _new_timeseries(self, jitter: float = 0):
        return copy.deepcopy(
            TimeSeries(
                normal_window_length=self._normal_window_length,
                abnormal_window_length=self._abnormal_window_length,
                k=self._k,
                minimum_sigma=jitter / self._k
            )
        )

    def _reset_ksigma(self):
        """
        根据收到的重置命令，重置ksigma算法的历史数据
        """

        now = datetime.datetime.now().timestamp()
        if now - self._last_reset < 10:
            return
        self._last_reset = now

        for idx, obj in self._instances.items():
            for metric, ts in obj[protocol.ATTR_METRICS].items():
                ts.reset()

    def run(self):
        logging.debug(f'Worker {self._name} 开始运行...')
        with ThreadPoolExecutor(max_workers=3) as pool:
            pool.submit(self._monitor_cmd_queue).add_done_callback(threading.thread_done_callback)
            pool.submit(self._monitor_data_queue).add_done_callback(threading.thread_done_callback)
            pool.submit(self._print_count).add_done_callback(threading.thread_done_callback)

    def _print_count(self):
        while True:
            if self._debug:
                logging.debug(f'Worker {self._name} 已处理元素: {self._count}')

            time.sleep(15)

    def _process_dashboard_request(self, attr: dict):
        """
        处理前端查询
        """

        result = {}

        for instance_idx, v in self._instances.items():
            zone, instance_type, idx = instance_idx

            result[instance_idx] = {
                protocol.ATTR_VALUE: None,
                protocol.ATTR_ABNORMAL:
                # bool(
                #     (int(datetime.datetime.now().timestamp()) - v[protocol.ATTR_ABNORMAL_STATE]) < self._cooldown
                # ),
                bool(v[protocol.ATTR_ABNORMAL_STATE] > 0),  # 一旦异常，就维持这个状态
                protocol.ATTR_FAILURE: bool(v[protocol.ATTR_FAILURE_STATE]),
            }

        return result

    def _monitor_cmd_queue(self):
        while True:
            cmd: command.Command = self._cmd_queue.get()
            attr = cmd.attributes
            if cmd.cmdType == command.CMD_TYPE_ABNORMAL_DETECTOR_QUERY:
                result = self._process_dashboard_request(attr)
                self._res_queue.put_nowait((cmd.cmdID, result))
            elif cmd.cmdType == command.CMD_TYPE_ABNORMAL_DETECTOR_RESET:
                self._reset_ksigma()

    def _monitor_data_queue(self):
        while True:
            element_list = self._data_queue.get()
            for element in element_list:
                self._count += 1
                instance_type = element[protocol.ATTR_INSTANCE_TYPE]
                zone = element[protocol.ATTR_ZONE]
                active = element[protocol.ATTR_ACTIVE]
                obj = element[protocol.ATTR_VALUE]
                idx = element[protocol.ATTR_ID]

                instance_idx = (zone, instance_type, idx)
                if instance_idx not in self._instances:
                    self._instances[instance_idx] = self._new_instance()

                self._instances[instance_idx][protocol.ATTR_HISTORY_VALUE] = obj
                self._instances[instance_idx][protocol.ATTR_FAILURE_STATE] = not active

                if not active:  # 不是active，则证明其已经属于failure，不属于abnormal
                    last_failure = self._instances[instance_idx][protocol.ATTR_LAST_FAILURE]
                    if datetime.datetime.now().timestamp() - last_failure >= self._cooldown:
                        if instance_type == protocol.INSTANCE_TYPE_SERVER:
                            self._add_anomaly_report(zone, protocol.ATTR_FAILURE, server_id=idx)
                        if instance_type == protocol.INSTANCE_TYPE_SWITCH:
                            self._add_anomaly_report(zone, protocol.ATTR_FAILURE, switch_id=idx)
                        if instance_type == protocol.INSTANCE_TYPE_LINK:
                            self._add_anomaly_report(zone, protocol.ATTR_FAILURE, link_id=idx)
                        self._instances[instance_idx][protocol.ATTR_LAST_FAILURE] = \
                            datetime.datetime.now().timestamp()
                else:
                    instance_dict = self._instances[instance_idx][protocol.ATTR_METRICS]

                    if instance_type == protocol.INSTANCE_TYPE_SERVER:  # 服务器，需要对其CPU和内存施行异常检测
                        if protocol.ATTR_SERVER_CPU_UTILIZATION not in instance_dict:
                            instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION] = self._new_timeseries(jitter=10)
                            instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION] = self._new_timeseries(jitter=5)

                        cpu_util_value = float(np.nanmean(obj.getCpuUtil()))
                        instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION].add(cpu_util_value)
                        mem_util_value = obj.getDRAMUsagePercentage()
                        instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION].add(mem_util_value)

                        abnormal = \
                            instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION].is_abnormal() or \
                            instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION].is_abnormal()

                        if self._debug and instance_idx[2] in (10001, 10002):
                            cpu_ts = instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION]
                            cpu_value = cpu_ts.value(8)
                            mem_ts = instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION]
                            mem_value = mem_ts.value(8)

                            mu, sigma = cpu_ts._stat_value.stats()
                            low = mu - sigma * self._k
                            high = mu + sigma * self._k
                            logging.info(f'{instance_idx[2]} CPU  {low:5.2f} {high:5.2f}')
                            print_s = ''
                            for item in cpu_value:
                                print_s += f'{item:.2f}, '
                            logging.info(print_s)

                            mu, sigma = mem_ts._stat_value.stats()
                            low = mu - sigma * self._k
                            high = mu + sigma * self._k
                            logging.info(f'{instance_idx[2]} MEM  {low:5.2f} {high:5.2f}')
                            print_s = ''
                            for item in mem_value:
                                print_s += f'{item:.2f}, '
                            logging.info(print_s)

                        if abnormal:
                            self._instances[instance_idx][protocol.ATTR_ABNORMAL_STATE] = \
                                int(datetime.datetime.now().timestamp())

                        last_abnormal = self._instances[instance_idx][protocol.ATTR_LAST_ABNORMAL]
                        if abnormal and datetime.datetime.now().timestamp() - last_abnormal >= self._cooldown:
                            if self._debug:
                                logging.info(f'server: {idx}\n'
                                             f'CPU: {instance_dict[protocol.ATTR_SERVER_CPU_UTILIZATION].value()}\n'
                                             f'memory:{instance_dict[protocol.ATTR_SERVER_MEMORY_UTILIZATION].value()}')
                            self._add_anomaly_report(zone, protocol.ATTR_ABNORMAL, server_id=idx)
                            self._instances[instance_idx][protocol.ATTR_LAST_ABNORMAL] = \
                                datetime.datetime.now().timestamp()

                    elif instance_type == protocol.INSTANCE_TYPE_LINK:  # 链路，需要对其SYN包等统计信息施行异常检测
                        if protocol.ATTR_LINK_SYN_RATIO not in instance_dict:
                            instance_dict[protocol.ATTR_LINK_SYN_RATIO] = self._new_timeseries(jitter=0)
                            instance_dict[protocol.ATTR_LINK_DNS_RATIO] = self._new_timeseries(jitter=0)

                        nsh_num_value = obj.NSH_num
                        syn_num_value = obj.SYN_num
                        dns_num_value = obj.DNS_num
                        link_util_value = obj.utilization
                        total_num_value = nsh_num_value + syn_num_value + dns_num_value

                        syn_ratio_value = syn_num_value / total_num_value if total_num_value > 0 else 0
                        instance_dict[protocol.ATTR_LINK_SYN_RATIO].add(syn_ratio_value)
                        dns_ratio_value = dns_num_value / total_num_value if total_num_value > 0 else 0
                        instance_dict[protocol.ATTR_LINK_DNS_RATIO].add(dns_ratio_value)

                        # util大于阈值，且DNS包或者SYN包的比例有大的变化
                        abnormal = \
                            link_util_value > self._link_util_thres and (
                                    instance_dict[protocol.ATTR_LINK_SYN_RATIO].is_abnormal() or
                                    instance_dict[protocol.ATTR_LINK_DNS_RATIO].is_abnormal() or
                                    syn_ratio_value > 0.95 or
                                    dns_ratio_value > 0.95
                            ) and (
                                    syn_num_value > self._link_packet_num_thres or
                                    dns_num_value > self._link_packet_num_thres
                            )

                        if abnormal:
                            self._instances[instance_idx][protocol.ATTR_ABNORMAL_STATE] = \
                                int(datetime.datetime.now().timestamp())

                            logging.debug(f'LINK ABNORMAL: {instance_idx[-1]} {link_util_value:.3f} '
                                          f'{instance_dict[protocol.ATTR_LINK_SYN_RATIO].is_abnormal()} '
                                          f'{instance_dict[protocol.ATTR_LINK_DNS_RATIO].is_abnormal()} '
                                          f'{syn_ratio_value:.2f} {dns_ratio_value:.2f}')

                        last_abnormal = self._instances[instance_idx][protocol.ATTR_LAST_ABNORMAL]
                        if abnormal and datetime.datetime.now().timestamp() - last_abnormal >= self._cooldown:
                            self._add_anomaly_report(zone, protocol.ATTR_ABNORMAL, link_id=idx)
                            self._instances[instance_idx][protocol.ATTR_LAST_ABNORMAL] = \
                                datetime.datetime.now().timestamp()
