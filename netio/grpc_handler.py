import time
import uuid
from abc import ABC
from typing import Union, Callable
from concurrent.futures import ThreadPoolExecutor
import logging

from sam.base import messageAgent as ma, request, command, exceptionProcessor
from sam.base.messageAgentAuxillary.msgAgentRPCConf import *
from sam.measurement import measurer

from netio import protocol
from util import Core

def thread_done_callback(worker):
    worker_exception = worker.exception()
    if worker_exception:
        exceptionProcessor.ExceptionProcessor(logging.getLogger()).logException(worker_exception)

class GRPCHandler(ABC):
    def __init__(self,
                 ip: str=ABNORMAL_DETECTOR_IP,
                 port: Union[str, int]=ABNORMAL_DETECTOR_PORT,
                 ):
        self._agent = ma.MessageAgent()
        self._ip = ip
        self._port = str(port)
        self._agent.startMsgReceiverRPCServer(self._ip, self._port)

        # 对于每种msg，有对应的处理函数
        self._recv_handlers = {}    # {msg_type: process_function}

        # 有些需要定时发送的消息，把对应的处理函数放置此处
        self._send_deadloop_handlers = []    # [(interval, process_function)]

        self._abnormal_results = []

    def regular_registration(self):
        """
        在实际生产环境中，注册以下的自动发送和消息处理函数。
        @return:
        """

        self.register_send_deadloop_handler(self._send_get_dcn_info, 5)
        self.register_send_deadloop_handler(self._send_abnormal_results)
        self.register_recv_handler(ma.MSG_TYPE_REPLY, self._recv_input_data)
        self.register_recv_handler(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD, self._recv_dashboard_request)

    def start_listening(self):
        """
        开始对RPC消息进行侦听。注意需要先注册对应类型消息，方可侦听。
        @return:
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
                    logging.warning(f'Unregistered message type: {msg_type}')

        ex = ThreadPoolExecutor(max_workers=1)
        ex.submit(_listen).add_done_callback(thread_done_callback)

    def start_deadloop_sending(self):
        """
        开始发送定时消息。注意需要先注册对应类型消息，方可发送。
        @return:
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
            logging.error(f'Message type {msg_type} registers None function')

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
        req = ma.Request(0, uuid.uuid1(), request.REQUEST_TYPE_GET_DCN_INFO)
        msg = ma.SAMMessage(ma.MSG_TYPE_REQUEST, req)
        logging.warning('Sending DCN request.')
        self._agent.sendMsgByRPC(DEFINABLE_MEASURER_IP, DEFINABLE_MEASURER_PORT, msg)

    def _send_abnormal_results(self):
        data = {
            protocol.ATTR_ALL_ZONE_DETECTION_DICT: {
                ma.TURBONET_ZONE: {
                    protocol.ATTR_FAILURE: {
                        protocol.ATTR_SWITCH_ID_LIST: [],
                        protocol.ATTR_SERVER_ID_LIST: [],
                        protocol.ATTR_LINK_ID_LIST: []
                    },
                    protocol.ATTR_ABNORMAL: {
                        protocol.ATTR_SWITCH_ID_LIST: [],
                        protocol.ATTR_SERVER_ID_LIST: [],
                        protocol.ATTR_LINK_ID_LIST: []
                    },
                },
                ma.SIMULATOR_ZONE: {
                    protocol.ATTR_FAILURE: {
                        protocol.ATTR_SWITCH_ID_LIST: [],
                        protocol.ATTR_SERVER_ID_LIST: [],
                        protocol.ATTR_LINK_ID_LIST: []
                    },
                    protocol.ATTR_ABNORMAL: {
                        protocol.ATTR_SWITCH_ID_LIST: [],
                        protocol.ATTR_SERVER_ID_LIST: [],
                        protocol.ATTR_LINK_ID_LIST: []
                    },
                }
            }
        }

        if len(self._abnormal_results) > 0:
            while len(self._abnormal_results) > 0:
                zone, type, switchID, serverID, linkID = self._abnormal_results[0]
                self._abnormal_results.pop(0)

                if switchID is not None:
                    data[protocol.ATTR_ALL_ZONE_DETECTION_DICT][zone][type][protocol.ATTR_SWITCH_ID_LIST].append(switchID)
                if serverID is not None:
                    data[protocol.ATTR_ALL_ZONE_DETECTION_DICT][zone][type][protocol.ATTR_SERVER_ID_LIST].append(serverID)
                if linkID is not None:
                    data[protocol.ATTR_ALL_ZONE_DETECTION_DICT][zone][type][protocol.ATTR_LINK_ID_LIST].append(linkID)

            cmd = command.Command(command.CMD_TYPE_HANDLE_FAILURE_ABNORMAL, uuid.uuid1(), attributes=data)
            msg = ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD, cmd)
            self._agent.sendMsgByRPC(REGULATOR_IP, REGULATOR_PORT, msg)

    def send_dashboard_reply(self, cmd_id: str, data: dict):
        cmd = command.CommandReply(cmd_id, command.CMD_STATE_SUCCESSFUL, attributes=data)
        msg = ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD_REPLY, cmd)
        self._agent.sendMsgByRPC(DASHBOARD_IP, DASHBOARD_PORT, msg)

    def _recv_dashboard_request(self, msg: ma.SAMMessage):
        msg_type = msg.getMessageType()
        assert msg_type == ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD

        cmd: command.Command = msg.getbody()
        attr = cmd.attributes
        if attr is None:
            logging.warning(f'Invalid dashboard request {cmd.cmdID}')
            return None

        Core.singleton().process_dashboard_request(cmd)

    def _recv_input_data(self, msg: ma.SAMMessage):
        msg_type = msg.getMessageType()
        assert msg_type == ma.MSG_TYPE_REPLY

        reply: request.Reply = msg.getbody()
        attr = reply.attributes
        if attr is None:
            logging.warning(f'Invalid input data reply {reply.requestID}')
            return None

        Core.singleton().process_input_data(reply)
