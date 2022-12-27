import datetime
import logging
import time
from abc import ABC
import numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Queue, Manager

from sam.base import sfc

from netio import protocol
from util import worker, threading


class Dispatcher(ABC):
    def __init__(
            self,
            k: float,
            data_queue: Queue,
            anom_queue: Queue,
            cmd_queue: Queue,
            res_queue: Queue,
            num_workers: int            =4,
            history_len_limit: int      =30,
            cooldown: int               =30,
            normal_window_length: int   =5,
            abnormal_window_length: int =2,
            debug: bool                 =False
    ):
        self._k = k
        self._num_workers = num_workers
        self._history_len_limit = history_len_limit
        self._cooldown = cooldown
        self._normal_window_length = normal_window_length
        self._abnormal_window_length = abnormal_window_length
        self._debug = debug

        self._ex = ProcessPoolExecutor(max_workers=self._num_workers)
        self._data_queue = data_queue
        self._anom_queue = anom_queue
        self._cmd_queue = cmd_queue
        self._res_queue = res_queue

        self._data_queues = [Manager().Queue() for _ in range(self._num_workers)]
        self._cmd_queues = [Manager().Queue() for _ in range(self._num_workers)]
        self._workers = [
            worker.Worker(
                k=self._k,
                data_queue=self._data_queues[idx],
                anom_queue=self._anom_queue,
                cmd_queue=self._cmd_queues[idx],
                res_queue=self._res_queue,
                history_len_limit=self._history_len_limit,
                cooldown=self._cooldown,
                normal_window_length=self._normal_window_length,
                abnormal_window_length=self._abnormal_window_length,
                debug=self._debug,
                name=f'w_{idx:02d}'
            )
            for idx in range(self._num_workers)
        ]

        self._instances_mapping = {}
        self._dispatch_time_costs = []
        self._process_time_costs = []
        self._instance_count = 0

    def _monitor_cmd_queue(self):
        while True:
            cmd = self._cmd_queue.get()
            for q in self._cmd_queues:
                q.put_nowait(cmd)

    def _monitor_data_queue(self):
        def func_dispatch_data(queue: Queue, _: dict):
            queue.put_nowait(_)
            return True

        while True:
            data = self._data_queue.get()
            t0 = time.time()
            timestamp = datetime.datetime.now().timestamp()

            data_queues_buffer = [[] for _ in range(self._num_workers)]
            for instance_type in protocol.INSTANCE_TYPES:
                if instance_type not in data:
                    continue
                instance_dict = data[instance_type]
                for zone in instance_dict.keys():
                    zone_dict = instance_dict[zone]
                    for idx, d in zone_dict.items():
                        instance_idx = (zone, instance_type, idx)

                        if instance_idx not in self._instances_mapping:
                            self._instances_mapping[instance_idx] = np.random.randint(0, self._num_workers)

                        obj = None
                        if type(d) == sfc.SFCI:
                            obj = d
                            active = True
                        else:
                            active = d[protocol.ATTR_ACTIVE]
                            if instance_type == protocol.INSTANCE_TYPE_SWITCH:
                                obj = d[protocol.ATTR_SWITCH]
                            elif instance_type == protocol.INSTANCE_TYPE_SERVER:
                                obj = d[protocol.ATTR_SERVER]
                            elif instance_type == protocol.INSTANCE_TYPE_LINK:
                                obj = d[protocol.ATTR_LINK]
                            elif instance_type == protocol.INSTANCE_TYPE_VNFI:
                                obj = d[protocol.ATTR_VNFI]

                        dispatch_data = {
                            protocol.ATTR_TIMESTAMP: timestamp,
                            protocol.ATTR_INSTANCE_TYPE: instance_type,
                            protocol.ATTR_ZONE: zone,
                            protocol.ATTR_ACTIVE: active,
                            protocol.ATTR_VALUE: obj,
                            protocol.ATTR_ID: idx
                        }
                        data_queues_buffer[self._instances_mapping[instance_idx]].append(dispatch_data)
                        self._instance_count += 1

            t1 = time.time()

            with ThreadPoolExecutor(max_workers=self._num_workers) as pool:
                for worker_idx in range(self._num_workers):
                    pool.submit(func_dispatch_data, self._data_queues[worker_idx], data_queues_buffer[worker_idx])
                pool.shutdown(wait=True)

            t2 = time.time()
            self._process_time_costs.append(t1 - t0)
            self._dispatch_time_costs.append(t2 - t1)

    def _print_desc(self):
        def avg(l: list) -> float:
            if len(l) == 0:
                return 0
            if len(l) < 5:
                return float(np.mean(l))
            return float(np.mean(l[-5:]))

        while True:
            sizes = list()
            for dq in self._data_queues:
                sizes.append(dq.qsize())
            avg_size = np.mean(sizes)

            if avg_size > 0:
                logging.info(f'Dispatcher 数据队列平均长度： {avg_size:.2f}')
            if self._data_queue.qsize() > 0:
                logging.info(f'输入数据队列长度: {self._data_queue.qsize()}')

            avg_dispatch_time = avg(self._dispatch_time_costs)
            avg_process_time = avg(self._process_time_costs)
            logging.info(f'处理: {avg_process_time:.2f}\t'
                         f'分发: {avg_dispatch_time:.2f}\t'
                         f'已处理: {self._instance_count}')

            time.sleep(20)

    def run(self):
        logging.info('Dispatcher 开始运行...')
        ex = ThreadPoolExecutor(max_workers=3)
        ex.submit(self._monitor_data_queue).add_done_callback(threading.thread_done_callback)
        ex.submit(self._print_desc).add_done_callback(threading.thread_done_callback)
        ex.submit(self._monitor_cmd_queue).add_done_callback(threading.thread_done_callback)

        with ProcessPoolExecutor(max_workers=self._num_workers) as pool:
            for w in self._workers:
                pool.submit(w.run).add_done_callback(threading.thread_done_callback)
