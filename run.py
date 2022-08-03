from netio.mock import MockDataSender, MockResultReceiver
from util import AnomalyDetector
from algo import KSigma

if __name__ == '__main__':
    s = MockDataSender(dst_queue_name='data')
    s.send()

    result_recv = MockResultReceiver(src_queue_name='result',
                                     process_func=print)
    result_recv.receive()

    ad = AnomalyDetector(src_queue_name='data',
                         dst_queue_name='result',
                         detector=KSigma())
    ad.main()
