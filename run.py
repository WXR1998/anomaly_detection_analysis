from util import Core
from algo import KSigma
from netio.grpc_handler import GRPCHandler

if __name__ == '__main__':
    core = Core(detector=KSigma(k=3), debug=True)

    handler = GRPCHandler()
    handler.regular_registration()
    handler.start_listening()
    handler.start_deadloop_sending()

    core.register_dashboard_reply_handler(handler.send_dashboard_reply)
    core.register_abnormal_result_handler(handler.add_anomaly_result)

    while True:
        pass
