import datetime
import json
from typing import Union

import numpy as np
from sam.base import messageAgent as ma
from netio import Sender
from .. import protocol

class MockDataSender(Sender):
    def __init__(self,
                 data: list[Union[np.ndarray, tuple, list]]=None,
                 metrics: list[str]=None,
                 interval: float=1,
                 dst_queue_name: str='data'):
        super().__init__(dst_queue_name, interval=interval)

        if data is None:
            data = [[0]]
            metrics = ['Metric']
        elif metrics is None:
            metrics = [f'Metric_{_}' for _ in range(len(data))]
        assert len(data) == len(metrics)

        self._data = data
        self._metrics = metrics
        self._cursor = np.zeros(len(data), dtype=int)

    def _send(self):
        """
        发送一个mock的信息
        """

        for idx, _ in enumerate(zip(self._metrics, self._data)):
            metric, data = _

            msg_body = {
                protocol.METRIC: metric,
                protocol.VALUE: data[self._cursor[idx]],
                protocol.LAST: int(datetime.datetime.now().timestamp()) - 60,
                protocol.CURRENT: int(datetime.datetime.now().timestamp())
            }
            self._agent.sendMsg(self._dst_queue_name, ma.SAMMessage(ma.MSG_TYPE_STRING, json.dumps(msg_body)))

            self._cursor[idx] = (self._cursor[idx] + 1) % len(data)
