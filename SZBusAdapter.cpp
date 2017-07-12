/*
 * SZBusAdapter.cpp
 *
 *  Created on: 2017年2月17日
 *      Author: wong
 */

#include "SZBusAdapter.h"

#include <cstring>
#include <stdio.h>

#include "base/Logging.h"
#include "BATNetSDKRawAPI.h"
#include "BATNetSDKAPI.h"
#include "SZBusProtocolAPI.h"
#include "RdkafkaProducer.h"
#include "FileWriter.h"
#include "Util.h"

#define CHECK_RET(exp) \
    { \
        int ret = exp; \
        if (ret != CCMS_RETURN_OK) \
        { \
            LOG_WARN << #exp << " = " << ret; \
            break; \
        } \
    }

SZBusAdapter::SZBusAdapter(const std::string& sn)
: m_hServer(-1)
, m_hClient(-1)
, m_producer(NULL)
, m_fileWriter(NULL)
{
    BATNetSDK_Init(CCMS_DEVTYPE_ADAPTER808, (char*)sn.c_str(), true);
}

SZBusAdapter::~SZBusAdapter()
{
    if (m_hServer != -1)
    {
        BATNetSDKRaw_DeleteObj(m_hServer);
    }
    if (m_hClient != -1)
    {
        BATNetSDK_DeleteObj(m_hClient);
    }
    BATNetSDK_Release();
}

void SZBusAdapter::SetProducer(RdkafkaProducer* producer, const std::string& topic)
{
    m_producer = producer;
    m_topic = topic;
}

void SZBusAdapter::SetFileWriter(FileWriter* fileWriter)
{
    m_fileWriter = fileWriter;
}

int SZBusAdapter::StartServer(const std::string& ip, uint16_t port)
{
    LOG_INFO << "StartServer " << ip << " " << port;
    CCMS_NETADDR addr = { {0}, 0};
    memcpy(addr.chIP, ip.c_str(), ip.length());
    addr.nPort = port;
    m_hServer = BATNetSDKRaw_CreateServerObj(&addr);
    BATNetSDKRaw_SetMsgCallBack(m_hServer, RecvCB, this);
    BATNetSDKRaw_Start(m_hServer);
    return 0;
}

int SZBusAdapter::StartClient(const std::string& ip, uint16_t port)
{
    LOG_INFO << "StartClient " << ip << " " << port;
    CCMS_NETADDR addr = { {0}, 0};
    memcpy(addr.chIP, ip.c_str(), ip.length());
    addr.nPort = port;
    m_hClient = BATNetSDK_CreateClientObj(&addr);
    BATNetSDK_Start(m_hClient);
    return 0;
}

int SZBusAdapter::OnRecv(int sessionId, const char* buf, int len)
{
//    printf("len=%d, ", len);
//    for (int i = 0; i < len; ++i)
//    {
//        printf("%02x ", (unsigned char)*(buf + i));
//    }
//    printf("\n");

    SZBusHeader header;
    const char* pBody = NULL;
    int parseLen = len, bodyLen = 0;
    int ret = SZBus_ParsePackage(buf, &parseLen, &header, &pBody, &bodyLen);
    LOG_DEBUG << "ret = " << ret << " parseLen = " << parseLen
        << " header: " << header.u1MsgId << " " << header.u2Length
        << " " << header.u1Sequence << " " << header.u1RecvRole << " " << header.u1SendRole
        << " " << header.u4RecvAddr << " " << header.u4SendAddr;
    if (ret == CCMS_RETURN_OK)
    {
        static time_t last = time(NULL);
        static size_t bytes = 0;
        bytes += parseLen;
        if (time(NULL) - last > 60)
        {
            LOG_INFO << "recv bytes: " << bytes;
            last = time(NULL);
            bytes = 0;
        }
        int packLen = SZBus_GetPackLen((SZBusMsg)header.u1MsgId);
        if (packLen <= 0)
        {
            LOG_WARN << "Unknown msg " << header.u1MsgId << " len = " << header.u2Length;
            return parseLen;
        }
        int bodyLen = SZBus_GetBodyLen((SZBusMsg)header.u1MsgId);
        int offset = SZBus_GetBodyOffset();
        char* outBuf = new char[packLen];
        switch (header.u1MsgId)
        {
        case SZBusMsg_PositionInfo:
            {
                PositionInfo positionInfo;
                CHECK_RET(SZBus_ParsePositionInfo(pBody, &bodyLen, &positionInfo));
                CHECK_RET(SZBus_PackPositionInfo(&positionInfo, outBuf + offset, &bodyLen));
                CHECK_RET(SZBus_PackPackage(header, outBuf + offset, bodyLen, outBuf, &packLen));
            }
            break;
        case SZBusMsg_ArriveStop:
            {
                ArriveStop arriveStop;
                CHECK_RET(SZBus_ParseArriveStop(pBody, &bodyLen, &arriveStop));
                CHECK_RET(SZBus_PackArriveStop(&arriveStop, outBuf + offset, &bodyLen));
                CHECK_RET(SZBus_PackPackage(header, outBuf + offset, bodyLen, outBuf, &packLen));
            }
            break;
        case SZBusMsg_LeaveStop:
            {
                LeaveStop leaveStop;
                CHECK_RET(SZBus_ParseLeaveStop(pBody, &bodyLen, &leaveStop));
                CHECK_RET(SZBus_PackLeaveStop(&leaveStop, outBuf + offset, &bodyLen));
                CHECK_RET(SZBus_PackPackage(header, outBuf + offset, bodyLen, outBuf, &packLen));
            }
            break;
        case SZBusMsg_ArriveStation:
            {
                ArriveStation arriveStation;
                CHECK_RET(SZBus_ParseArriveStation(pBody, &bodyLen, &arriveStation));
                CHECK_RET(SZBus_PackArriveStation(&arriveStation, outBuf + offset, &bodyLen));
                CHECK_RET(SZBus_PackPackage(header, outBuf + offset, bodyLen, outBuf, &packLen));
            }
            break;
        case SZBusMsg_LeaveStation:
            {
                LeaveStation leaveStation;
                CHECK_RET(SZBus_ParseLeaveStation(pBody, &bodyLen, &leaveStation));
                CHECK_RET(SZBus_PackLeaveStation(&leaveStation, outBuf + offset, &bodyLen));
                CHECK_RET(SZBus_PackPackage(header, outBuf + offset, bodyLen, outBuf, &packLen));
            }
            break;
        case SZBusMsg_AlarmInfo:
            {
                AlarmInfo alarmInfo;
                CHECK_RET(SZBus_ParseAlarmInfo(pBody, &bodyLen, &alarmInfo));
                CHECK_RET(SZBus_PackAlarmInfo(&alarmInfo, outBuf + offset, &bodyLen));
                CHECK_RET(SZBus_PackPackage(header, outBuf + offset, bodyLen, outBuf, &packLen));
            }
            break;
        default:
            assert(false);
            break;
        }
        //m_sendQueue.push(outBuf);
        BATNetSDK_SendData(m_hClient, 0, CCMS_SZBUSGPS_MSG, outBuf, packLen);
        SAFE_DELETEA(outBuf);
        return parseLen;
    }
    else if (ret == CCMS_RETURN_CONTINUE)
    {
        return 0;
    }
    else
    {
        LOG_WARN << "ret = " << ret << " len = " << len << " parseLen = " << parseLen;
        return -1;
    }
}

int SZBusAdapter::OnRecv2(int sessionId, const char* buf, int len)
{
//    Util::HexDump(buf, len);

    SZBusHeader header;
    const char* pBody = NULL;
    int parseLen = len, bodyLen = 0;
    int ret = SZBus_ParsePackage(buf, &parseLen, &header, &pBody, &bodyLen);
    LOG_DEBUG << "ret = " << ret << " parseLen = " << parseLen
        << " header: " << header.u1MsgId << " " << header.u2Length
        << " " << header.u1Sequence << " " << header.u1RecvRole << " " << header.u1SendRole
        << " " << header.u4RecvAddr << " " << header.u4SendAddr;
    if (ret == CCMS_RETURN_OK)
    {
//        Util::HexDump(pBody, bodyLen);
        static time_t last = time(NULL);
        static size_t bytes = 0;
        bytes += parseLen;
        if (time(NULL) - last > 60)
        {
            LOG_INFO << "recv bytes: " << bytes;
            last = time(NULL);
            bytes = 0;
        }
        int packLen = SZBus_GetPackLen((SZBusMsg)header.u1MsgId);
        if (packLen <= 0)
        {
            LOG_WARN << "Unknown msg " << header.u1MsgId << " len = " << header.u2Length;
            return parseLen;
        }
        int bodyLen = SZBus_GetBodyLen((SZBusMsg)header.u1MsgId);
        int strlen = 0;
        char* outBuf = NULL;
        std::string key;
        switch (header.u1MsgId)
        {
        case SZBusMsg_PositionInfo:
            {
                PositionInfo positionInfo;
                CHECK_RET(SZBus_ParsePositionInfo(pBody, &bodyLen, &positionInfo));
                SZBus_PositionInfo2Str(&positionInfo, NULL, &strlen);
                outBuf = new char[++strlen];
                CHECK_RET(SZBus_PositionInfo2Str(&positionInfo, outBuf, &strlen));
                key = "PositionInfo";
            }
            break;
        case SZBusMsg_ArriveStop:
            {
                ArriveStop arriveStop;
                CHECK_RET(SZBus_ParseArriveStop(pBody, &bodyLen, &arriveStop));
                SZBus_ArriveStop2Str(&arriveStop, NULL, &strlen);
                outBuf = new char[++strlen];
                CHECK_RET(SZBus_ArriveStop2Str(&arriveStop, outBuf, &strlen));
                key = "ArriveStop";
            }
            break;
        case SZBusMsg_LeaveStop:
            {
                LeaveStop leaveStop;
                CHECK_RET(SZBus_ParseLeaveStop(pBody, &bodyLen, &leaveStop));
                SZBus_LeaveStop2Str(&leaveStop, NULL, &strlen);
                outBuf = new char[++strlen];
                CHECK_RET(SZBus_LeaveStop2Str(&leaveStop, outBuf, &strlen));
                key = "LeaveStop";
            }
            break;
        case SZBusMsg_ArriveStation:
            {
                ArriveStation arriveStation;
                CHECK_RET(SZBus_ParseArriveStation(pBody, &bodyLen, &arriveStation));
                SZBus_ArriveStation2Str(&arriveStation, NULL, &strlen);
                outBuf = new char[++strlen];
                CHECK_RET(SZBus_ArriveStation2Str(&arriveStation, outBuf, &strlen));
                key = "ArriveStation";
            }
            break;
        case SZBusMsg_LeaveStation:
            {
                LeaveStation leaveStation;
                CHECK_RET(SZBus_ParseLeaveStation(pBody, &bodyLen, &leaveStation));
                SZBus_LeaveStation2Str(&leaveStation, NULL, &strlen);
                outBuf = new char[++strlen];
                CHECK_RET(SZBus_LeaveStation2Str(&leaveStation, outBuf, &strlen));
                key = "LeaveStation";
            }
            break;
        case SZBusMsg_AlarmInfo:
            {
                AlarmInfo alarmInfo;
                CHECK_RET(SZBus_ParseAlarmInfo(pBody, &bodyLen, &alarmInfo));
                SZBus_AlarmInfo2Str(&alarmInfo, NULL, &strlen);
                outBuf = new char[++strlen];
                CHECK_RET(SZBus_AlarmInfo2Str(&alarmInfo, outBuf, &strlen));
                key = "AlarmInfo";
            }
            break;
        default:
            assert(false);
            break;
        }
        if (!key.empty()) {
          //m_sendQueue.push(outBuf);
          if (m_hClient != -1) {
            BATNetSDK_SendData(m_hClient, 0, CCMS_SZBUSGPS_MSG, buf, packLen);
          }
          if (m_producer != NULL) {
            LOG_DEBUG << "kafka " << key << " " << strlen << " " << outBuf;
            m_producer->Produce(m_topic, key, outBuf, strlen);
          }
          if (m_fileWriter != NULL) {
            m_fileWriter->WriteLine(3, key.c_str(), ",", outBuf);
          }
        }
        SAFE_DELETEA(outBuf);
        return parseLen;
    }
    else if (ret == CCMS_RETURN_CONTINUE)
    {
        return 0;
    }
    else
    {
        LOG_WARN << "ret = " << ret << " len = " << len << " parseLen = " << parseLen;
        return -1;
    }
}

