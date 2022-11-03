import uuid

from sam.base import command, messageAgent as ma
from sam.base.messageAgentAuxillary.msgAgentRPCConf import *

from netio import protocol

if __name__ == '__main__':
    agent = ma.MessageAgent()
    agent.startMsgReceiverRPCServer(ABNORMAL_DETECTOR_IP, 30012)

    cmd = command.Command(
        cmdType=command.CMD_TYPE_ABNORMAL_DETECTOR_QUERY,
        cmdID=uuid.UUID(int=1),
        attributes={
            protocol.ATTR_ZONE: 'SIMULATOR_ZONE'
        }
    )
    msg = ma.SAMMessage(ma.MSG_TYPE_ABNORMAL_DETECTOR_CMD, cmd)
    agent.sendMsgByRPC(ABNORMAL_DETECTOR_IP, ABNORMAL_DETECTOR_PORT, msg)
