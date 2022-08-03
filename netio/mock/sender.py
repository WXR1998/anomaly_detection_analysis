import datetime
import json

from sam.base import messageAgent as ma
from netio import Sender
from .. import protocol

class MockDataSender(Sender):
    def __init__(self, dst_queue_name: str='data'):
        super().__init__(dst_queue_name)

    def _send(self):
        """
        发送一个mock的信息
        """
        data = {
            protocol.METRIC: 'kpi_test',
            protocol.VALUE: 1234,
            protocol.LAST: int(datetime.datetime.now().timestamp()) - 60,
            protocol.CURRENT: int(datetime.datetime.now().timestamp())
        }
        self._agent.sendMsg(self._dst_queue_name, ma.SAMMessage(ma.MSG_TYPE_STRING, json.dumps(data)))