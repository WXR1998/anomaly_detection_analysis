import numpy as np
import logging

from netio.mock import MockDataSender, MockResultReceiver
from util import AnomalyDetector
from algo import KSigma

if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
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
