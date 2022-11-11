import time
import uuid
import copy
from abc import ABC
from typing import Union, Callable
from concurrent.futures import ThreadPoolExecutor
import logging
import json

from sam.base import messageAgent as ma, request, command, exceptionProcessor
from sam.base.messageAgentAuxillary.msgAgentRPCConf import *
import sam.base

from netio import protocol
from util import Core

def thread_done_callback(worker):
    worker_exception = worker.exception()
    if worker_exception:
        exceptionProcessor.ExceptionProcessor(logging.getLogger()).logException(worker_exception)

def empty_anomaly_report():
    data = {
        ma.TURBONET_ZONE: {
            protocol.ATTR_FAILURE: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
            protocol.ATTR_ABNORMAL: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
        },
        ma.SIMULATOR_ZONE: {
            protocol.ATTR_FAILURE: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
            protocol.ATTR_ABNORMAL: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
        }
    }
    return copy.deepcopy(data)

class GRPCHandler(ABC):
    def __init__(self,
                 interval: float=3.0,
                 ip: str=ABNORMAL_DETECTOR_IP,
                 port: Union[str, int]=ABNORMAL_DETECTOR_PORT,
                 send_results: bool=False
                 ):
        """
        Args:
            interval:       从measurer获取数据的间隔
            ip:
            port:
            send_results:
        """

        self._agent = ma.MessageAgent()
        self._ip = ip
        self._port = str(port)
        self._agent.startMsgReceiverRPCServer(self._ip, self._port)
        self._send_results = send_results
        self._interval = interval

        # 对于每种msg，有对应的处理函数
        self._recv_handlers = {}    # {msg_type: process_function}

        # 有些需要定时发送的消息，把对应的处理函数放置此处
        self._send_deadloop_handlers = []    # [(interval, process_function)]

        self._abnormal_results = []
        self._topo = None

    def regular_registration(self):
        """
        在实际生产环境中，注册以下的自动发送和消息处理函数。
        """

        self.register_send_deadloop_handler(self._send_get_dcn_info, self._interval)
        self.register_send_deadloop_handler(self._send_abnormal_results)
        self.register_recv_handler(ma.MSG_TYPE_REPLY, self._recv_input_data)
        self.register_recv_handler(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD, self._recv_cmd)

    def start_listening(self):
        """
        开始对RPC消息进行侦听。注意需要先注册对应类型消息，方可侦听。
        """

        def _process_thread(msg_type: str, msg: ma.SAMMessage):
            self._recv_handlers[msg_type](msg)

        def _listen():
            ex = ThreadPoolExecutor(max_workers=8)
            while True:
                msg = self._agent.getMsgByRPC(self._ip, self._port)
                msg_type = msg.getMessageType()
                if msg_type is None:
                    continue

                if msg_type in self._recv_handlers:
                    ex.submit(_process_thread, msg_type, msg).add_done_callback(thread_done_callback)
                else:
                    logging.warning(f'未注册的消息类型：{msg_type}')

        ex = ThreadPoolExecutor(max_workers=1)
        ex.submit(_listen).add_done_callback(thread_done_callback)

    def start_deadloop_sending(self):
        """
        开始发送定时消息。注意需要先注册对应类型消息，方可发送。
        """

        def _wrapped_func(interval: Union[int, float], func: Callable):
            ex = ThreadPoolExecutor(max_workers=8)
            while True:
                ex.submit(func).add_done_callback(thread_done_callback)
                if interval > 0:
                    time.sleep(interval)

        ex = ThreadPoolExecutor(max_workers=max(1, len(self._send_deadloop_handlers)))
        for _ in self._send_deadloop_handlers:
            interval, func = _
            ex.submit(_wrapped_func, interval, func).add_done_callback(thread_done_callback)

    def register_recv_handler(self, msg_type: str, handler: Callable=None):
        if handler is not None:
            self._recv_handlers[msg_type] = handler
        else:
            logging.error(f'消息类型 {msg_type} 未注册处理函数')

    def register_send_deadloop_handler(self, func: Callable, interval: Union[int, float]=1):
        self._send_deadloop_handlers.append((interval, func))

    def add_anomaly_result(self, zone: str, type: str,
                           switch_id: str = None, server_id: str = None, link_id: tuple = None):
        """
        给某个组件添加一个anomaly，不能三个全为None
        """

        assert zone in [ma.TURBONET_ZONE, ma.SIMULATOR_ZONE]
        assert type in [protocol.ATTR_ABNORMAL, protocol.ATTR_FAILURE]
        self._abnormal_results.append((zone, type, switch_id, server_id, link_id))

    def _send_get_dcn_info(self):
        req = request.Request(0, uuid.uuid1(), request.REQUEST_TYPE_GET_DCN_INFO)
        msg = ma.SAMMessage(ma.MSG_TYPE_REQUEST, req)
        logging.debug('发送DCN请求...')
        self._agent.sendMsgByRPC(MEASURER_IP, MEASURER_PORT, msg)

    def _data_format(self, d: dict, limit: int=10) -> str:
        """
        将dict格式化输出，较长的list只显示前n项
        """
        result = None

        if type(d) == dict:
            for k, v in d.items():
                r = f'{k}: {self._data_format(v)}'
                if result is None:
                    result = r
                else:
                    result = result + ', ' + r
            result = '{' + result + '}'
        elif type(d) == list:
            l = len(d)
            if len(d) > limit:
                d = d[:limit]
            result = f'{json.dumps(d)}' + ('' if l <= limit else f' (len: {l})')
        return result

    def _send_abnormal_results(self):
        if len(self._abnormal_results) == 0:
            return

        data = empty_anomaly_report()
        while len(self._abnormal_results) > 0:
            zone, type_, switchID, serverID, linkID = self._abnormal_results[0]
            self._abnormal_results.pop(0)

            if switchID is not None:
                data[zone][type_][protocol.ATTR_SWITCH_ID_LIST].add(switchID)
            if serverID is not None:
                data[zone][type_][protocol.ATTR_SERVER_ID_LIST].add(serverID)
            if linkID is not None:
                data[zone][type_][protocol.ATTR_LINK_ID_LIST].add(linkID)

        # 交换机故障推断
        # for zone in self._topo.keys():
        #     for src_id in self._topo[zone]:
        #         count = 0
        #         ano_count = 0
        #         for dst_id in self._topo[zone][src_id]:
        #             if (src_id, dst_id) in data[zone][protocol.ATTR_ABNORMAL][protocol.ATTR_LINK_ID_LIST]:
        #                 ano_count += 1
        #             count += 1
        #
        #         if ano_count > int(count * 0.8):        # 该交换机发出的超过80%的link有问题，需要将告警合并
        #             data[zone][protocol.ATTR_ABNORMAL][protocol.ATTR_SWITCH_ID_LIST].add(src_id)
        #             for dst_id in self._topo[zone][src_id]:
        #                 obj = (src_id, dst_id)
        #                 data[zone][protocol.ATTR_ABNORMAL][protocol.ATTR_LINK_ID_LIST].discard(obj)

        data = {
            protocol.ATTR_ALL_ZONE_DETECTION_DICT: {
                zone: {
                    anomaly_type: {
                        id_list_key: list(id_list)
                        for id_list_key, id_list in anomaly_desc.items()
                    } for anomaly_type, anomaly_desc in zone_desc.items()
                } for zone, zone_desc in data.items()
            }
        }

        logging.warning('故障报警结果：\t' + self._data_format(data))

        cmd = command.Command(command.CMD_TYPE_HANDLE_FAILURE_ABNORMAL, uuid.uuid1(), attributes=data)
        msg = ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD, cmd)
        if self._send_results:
            self._agent.sendMsgByRPC(REGULATOR_IP, REGULATOR_PORT, msg)


    def send_dashboard_reply(self, cmd_id: uuid.UUID, data: dict):
        cmd = command.CommandReply(cmd_id, command.CMD_STATE_SUCCESSFUL, attributes=data)
        msg = ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD_REPLY, cmd)
        self._agent.sendMsgByRPC(DASHBOARD_IP, DASHBOARD_PORT, msg)

    def _recv_cmd(self, msg: ma.SAMMessage):
        msg_type = msg.getMessageType()
        assert msg_type == ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD

        cmd: command.Command = msg.getbody()
        attr = cmd.attributes
        if cmd.cmdType == command.CMD_TYPE_ABNORMAL_DETECTOR_QUERY:
            if attr is None:
                logging.warning(f'非法前端查询请求 {cmd.cmdID}')
                return None
            Core.singleton().process_dashboard_request(cmd)

        elif cmd.cmdType == command.CMD_TYPE_ABNORMAL_DETECTOR_RESET:
            Core.singleton().reset_ksigma()

    def _recv_input_data(self, msg: ma.SAMMessage):
        msg_type = msg.getMessageType()
        assert msg_type == ma.MSG_TYPE_REPLY

        reply: request.Reply = msg.getbody()
        attr = reply.attributes
        if attr is None:
            logging.warning(f'非法输入数据 {reply.requestID}')
            return None

        Core.singleton().process_input_data(reply)

    def _recv_reset(self, msg: ma.SAMMessage):
        msg_type = msg.getMessageType()
        assert msg_type == ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD
        logging.warning('发布新VNFI，重置ksigma参数')

        Core.singleton().reset_ksigma()

    def _set_topo(self, topo: dict):
        self._topo = topo
        logging.warning('交换机链路拓扑已获取')
