import json

from netio import Receiver


class MockResultReceiver(Receiver):
    def __init__(self, src_queue_name: str='result', process_func=None):
        super().__init__(src_queue_name, process_func)

    @staticmethod
    def parse_data(data_str: str):
        """
        处理输入数据，将其分拆成 metric, value, last, current 四个值
        """
        obj = json.loads(data_str)

        return obj
