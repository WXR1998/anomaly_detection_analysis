from util import Core
from netio.grpc_handler import GRPCHandler
import logging

logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] %(levelname)s:\t%(message)s',
                    datefmt='%m-%d %H:%M:%S',
                    level=logging.INFO)

if __name__ == '__main__':
    core = Core(
        k=8,                                # k-sigma算法的k
        normal_window_length=8,             # 以时间序列的前多少个元素确定k-sigma算法的标准值
        abnormal_window_length=3,           # 时间序列的后若干个点均为异常，则报出整体的异常
        cooldown=30,                        # 对于每个instance的异常，多少秒内只报告一次
        debug=False                         # 是否打印数据处理进度条
    )

    handler = GRPCHandler(
        interval=3.0,                       # 每interval秒获取并处理一次数据
        send_results=False,                 # 是否要向regulator报告abnormal和failure
    )
    handler.regular_registration()
    handler.start_listening()
    handler.start_deadloop_sending()

    core.register_dashboard_reply_handler(handler.send_dashboard_reply)
    core.register_abnormal_result_handler(handler.add_anomaly_result)
    core.register_topo_handler(handler._set_topo)

    while True:
        pass
