import datetime
import random
import time
import uuid
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue
import logging

from sam.base.command import CMD_TYPE_ABNORMAL_DETECTOR_RESET
from sam.base.messageAgentAuxillary.msgConstant import MSG_TYPE_ABNORMAL_DETECTOR_CMD

from netio import protocol
from util import anomaly_report, threading

from sam.base import command, messageAgent as ma, request
from sam.base.messageAgentAuxillary.msgAgentRPCConf import ABNORMAL_DETECTOR_IP, ABNORMAL_DETECTOR_PORT, SIMULATOR_IP, \
    SIMULATOR_PORT, MEASURER_IP, MEASURER_PORT, REGULATOR_IP, REGULATOR_PORT, DASHBOARD_IP, DASHBOARD_PORT


class IOHandler(ABC):
    SFCI_NAME = 'sfcisDict'
    DATA_SOURCE = {
        ma.MSG_TYPE_SIMULATOR_CMD_REPLY: ma.SIMULATOR_ZONE,
        ma.MSG_TYPE_REPLY: ma.TURBONET_ZONE
    }

    def __init__(
            self,
            interval: float,
            num_workers: int,
            data_queue: Queue,      # 用于接收输入数据，并将其送到主进程
            anom_queue: Queue,      # 用于接收异常告警信息，并将其发送给调度器
            cmd_queue:  Queue,      # 用于接收命令消息，并将其送到主进程
            res_queue:  Queue,      # 用于接收前端返回的查询结果，并将其发送给前端
            send_reports: bool=True # 是否要发送异常告警
    ):

        self._agent = None
        self._interval = interval
        self._num_workers = num_workers

        self._data_queue = data_queue
        self._anom_queue = anom_queue
        self._cmd_queue = cmd_queue
        self._res_queue = res_queue

        self._send_reports = send_reports
        self._receive_message_type = (
            ma.MSG_TYPE_SIMULATOR_CMD_REPLY,
            ma.MSG_TYPE_REPLY,
        )

        self._last_recv_timestamp = {
            k: datetime.datetime.now().timestamp() for
            k in self._receive_message_type
        }

        self._dashboard_command_results = {}

    def _send_simulator(self):
        """
        发送一次Simulator的取数请求
        """

        getSFCIStateCmd = command.Command(
            cmdType=command.CMD_TYPE_GET_ALL_INFO,
            cmdID=uuid.uuid1(),
            attributes={'zone': ma.SIMULATOR_ZONE}
        )
        msg = ma.SAMMessage(ma.MSG_TYPE_SIMULATOR_CMD, getSFCIStateCmd)
        logging.debug('发送Simulator Data请求...')
        self._agent.sendMsgByRPC(SIMULATOR_IP, SIMULATOR_PORT, msg, maxRetryNum=0)

    def _send_turbonet(self):
        """
        发送一次Measurer的取数请求
        """

        req = request.Request(
            userID=0,
            requestID=uuid.uuid1(),
            requestType=request.REQUEST_TYPE_GET_DCN_INFO
        )
        msg = ma.SAMMessage(ma.MSG_TYPE_REQUEST, req)
        logging.debug('发送Turbonet Data请求...')
        self._agent.sendMsgByRPC(MEASURER_IP, MEASURER_PORT, msg, maxRetryNum=0)

    def _send_request(self):
        """
        发送一次取数请求
        """

        self._send_turbonet()
        self._send_simulator()

    def _send_reset(self):
        """
        发送一次重置命令
        """
        resetCmd = command.Command(CMD_TYPE_ABNORMAL_DETECTOR_RESET, uuid.uuid1())
        msg = ma.SAMMessage(MSG_TYPE_ABNORMAL_DETECTOR_CMD, resetCmd)
        self._agent.sendMsgByRPC(ABNORMAL_DETECTOR_IP, ABNORMAL_DETECTOR_PORT, msg, maxRetryNum=0)

    def _send_anomaly_report(self, report: dict):
        """
        发送一次报障结果
        """

        logging.warning('故障报警结果：\t' + anomaly_report.data_format(report))
        if not self._send_reports:
            return

        cmd = command.Command(
            cmdType=command.CMD_TYPE_HANDLE_FAILURE_ABNORMAL,
            cmdID=uuid.uuid1(),
            attributes=report
        )
        msg = ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD, cmd)
        self._agent.sendMsgByRPC(REGULATOR_IP, REGULATOR_PORT, msg, maxRetryNum=0)

    def _send_dashboard_reply(self, cmd_id, cmd_attr: dict):
        """
        发送一次前端查询结果
        """

        try:
            logging.info(f'发送前端查询结果 {cmd_id}')
            cmd = command.CommandReply(
                cmdID=cmd_id,
                cmdState=command.CMD_STATE_SUCCESSFUL,
                attributes=cmd_attr
            )
            msg = ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD_REPLY, cmd)
            self._agent.sendMsgByRPC(DASHBOARD_IP, DASHBOARD_PORT, msg, maxRetryNum=0)
        except Exception as e:
            logging.warning(f'发送前端结果非法 {e}')

    def _monitor_anomaly_report(self):
        """
        处理告警队列
        [阻塞方法]
        """

        while True:
            report = anomaly_report.empty_anomaly_report_set()
            count = 0
            while not self._anom_queue.empty():
                anom = self._anom_queue.get()
                zone, type_, switchID, serverID, linkID = anom
                count += 1

                if switchID is not None:
                    report[zone][type_][protocol.ATTR_SWITCH_ID_LIST].add(switchID)
                if serverID is not None:
                    report[zone][type_][protocol.ATTR_SERVER_ID_LIST].add(serverID)
                if linkID is not None:
                    report[zone][type_][protocol.ATTR_LINK_ID_LIST].add(linkID)

            if count > 0:
                report = anomaly_report.get_anomaly_report_list(report)
                self._send_anomaly_report(report)

            time.sleep(1)

    def _monitor_dashboard_reply(self):
        """
        处理前端查询结果
        因为有多个worker，需要把每个worker返回的结果合并起来返回
        [阻塞方法]
        """

        while True:
            try:
                cmd_id, cmd_attr = self._res_queue.get()
                if cmd_id not in self._dashboard_command_results:
                    self._dashboard_command_results[cmd_id] = []
                self._dashboard_command_results[cmd_id].append(cmd_attr)

                if len(self._dashboard_command_results[cmd_id]) == self._num_workers:   # 所有worker都已返回查询结果
                    results = self._dashboard_command_results[cmd_id]
                    r = {}
                    for item in results:
                        r.update(item)

                    formatted_results = {
                        zone: {
                            instance_type: {}
                            for instance_type in protocol.INSTANCE_TYPES
                        } for zone in protocol.ZONES
                    }
                    for idx, v in r.items():
                        zone = v[protocol.ATTR_ZONE]
                        instance_type = v[protocol.ATTR_INSTANCE_TYPE]
                        v.pop(protocol.ATTR_ZONE)
                        v.pop(protocol.ATTR_INSTANCE_TYPE)
                        formatted_results[zone][instance_type][idx] = v

                    self._send_dashboard_reply(cmd_id, formatted_results)
                    self._dashboard_command_results.pop(cmd_id)

            except Exception as e:
                logging.warning(f'前端请求结果发生错误 {e}')

    def _monitor_recv_data(self):
        """
        从Simulator和Measurer处接收数据，或接收重置信息和前端查询
        [阻塞方法]
        """

        def postprocessing(_: dict):
            if self.SFCI_NAME in _.keys():
                _[protocol.INSTANCE_TYPE_SFCI] = _[self.SFCI_NAME]
                _.pop(self.SFCI_NAME)
            return _

        while True:
            try:
                msg = self._agent.getMsgByRPC(ABNORMAL_DETECTOR_IP, ABNORMAL_DETECTOR_PORT)
                msg_type = msg.getMessageType()
                if msg_type in self._receive_message_type:
                    body = msg.getbody()
                    data = body.attributes
                    data = postprocessing(data)

                    now = datetime.datetime.now().timestamp()
                    logging.info(f'收到{self.DATA_SOURCE[msg_type]}\t数据 '
                                 f'({now - self._last_recv_timestamp[msg_type]:.2f}s)')
                    self._last_recv_timestamp[msg_type] = now

                    self._data_queue.put(data)
                elif msg_type == ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD:
                    cmd: command.Command = msg.getbody()
                    attr = cmd.attributes
                    if cmd.cmdType == command.CMD_TYPE_ABNORMAL_DETECTOR_QUERY:
                        if attr is not None:
                            logging.info(f'收到前端查询')
                            self._cmd_queue.put(cmd)
                    elif cmd.cmdType == command.CMD_TYPE_ABNORMAL_DETECTOR_RESET:
                        logging.warning(f'重置算法历史数据')
                        self._cmd_queue.put(cmd)
            except Exception as e:
                logging.warning(f'接收数据非法 {e}')
            time.sleep(0.1)

    def run(self):
        logging.info('IOHandler 开始运行...')
        if self._agent is None:
            self._agent = ma.MessageAgent()
            self._agent.startMsgReceiverRPCServer(ABNORMAL_DETECTOR_IP, ABNORMAL_DETECTOR_PORT)

        with ThreadPoolExecutor(max_workers=4) as pool:
            pool.submit(self._monitor_recv_data).add_done_callback(threading.thread_done_callback)
            pool.submit(self._monitor_dashboard_reply).add_done_callback(threading.thread_done_callback)
            pool.submit(self._monitor_anomaly_report).add_done_callback(threading.thread_done_callback)

            while True:
                t0 = datetime.datetime.now().timestamp()

                pool.submit(self._send_request).add_done_callback(threading.thread_done_callback)

                t1 = datetime.datetime.now().timestamp()
                time.sleep(self._interval - (t1 - t0))
