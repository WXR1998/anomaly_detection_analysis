import numpy as np
import logging

from util.dataset import Dataset

logging.basicConfig(level=logging.WARNING)

from netio.mock import MockDataSender, MockResultReceiver
from util import AnomalyDetector
from algo import KSigma

def mock_test():
    data = np.concatenate([np.random.normal(0, 1, 60), np.random.normal(10, 1, 60)])

    data_send = MockDataSender(dst_queue_name='data',
                               data=[data],
                               metrics=['test metric 1'],
                               interval=0.01
                               )
    data_send.send()

    res_recv = MockResultReceiver(src_queue_name='result',
                                  process_func=print)
    res_recv.receive()

    ad = AnomalyDetector(src_queue_name='data',
                         dst_queue_name='result',
                         detector=KSigma())
    ad.main()

if __name__ == '__main__':
    dataset = Dataset()
    dataset.add_csv('./data/cpu_data.csv')
    dataset.add_csv('./data/mem_data.csv')

    names, values = [], []
    for data_name, _ in dataset.data().items():
        timestamp, value = _
        names.append(data_name)
        values.append(value)

    sender = MockDataSender(dst_queue_name='data',
                            data=values,
                            metrics=names,
                            interval=0.01)
    sender.send()

    recv = MockResultReceiver(src_queue_name='result',
                                  process_func=print)
    recv.receive()

    ad = AnomalyDetector(src_queue_name='data',
                         dst_queue_name='result',
                         detector=KSigma(k=3))
    ad.main()
