import datetime
import json
from typing import Union

import numpy as np
from sam.base import messageAgent as ma
from sam.base import switch, server, sfc, vnf, link
from netio import Sender

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

            msg_body = None
            type_idx = np.random.randint(0, 5)

            if type_idx == 0:
                msg_body = switch.Switch(
                    switchID=0,
                    switchType=switch.SWITCH_TYPE_FORWARD,
                )
            elif type_idx == 1:
                msg_body = server.Server(
                    controlIfName='ens0',
                    datapathIfIP='192.168.44.123',
                    serverType=server.SERVER_TYPE_NORMAL
                )
            elif type_idx == 2:
                msg_body = sfc.SFCI(
                    sfciID=0,
                )
            elif type_idx == 3:
                msg_body = vnf.VNFI(
                    vnfID=123,
                    vnfiID=1,
                )
            elif type_idx == 4:
                msg_body = link.Link(
                    srcID=0,
                    dstID=1,
                    utilization=0.23,
                )

            self._agent.sendMsg(self._dst_queue_name, ma.SAMMessage(ma.MSG_TYPE_STRING, msg_body))

            self._cursor[idx] = (self._cursor[idx] + 1) % len(data)
