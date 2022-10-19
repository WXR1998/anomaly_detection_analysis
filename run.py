from util import Core
from algo import KSigma
from netio.grpc_handler import GRPCHandler
import logging

logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] %(levelname)s:\t%(message)s',
                    datefmt='%m-%d %H:%M:%S')

if __name__ == '__main__':
    core = Core(detector=KSigma(k=5),
                normal_window_length=10,
                debug=False)

    handler = GRPCHandler(send_results=False)   # Whether to send results to the regulator
    handler.regular_registration()
    handler.start_listening()
    handler.start_deadloop_sending()

    core.register_dashboard_reply_handler(handler.send_dashboard_reply)
    core.register_abnormal_result_handler(handler.add_anomaly_result)
    core.register_topo_handler(handler._set_topo)

    while True:
        pass
