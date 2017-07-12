/*
 * SZBusAdapter.h
 *
 *  Created on: 2017年2月17日
 *      Author: wong
 */

#ifndef SZBUSADAPTER_H_
#define SZBUSADAPTER_H_

#include <stdint.h>
#include <string>

class RdkafkaProducer;
class FileWriter;

class SZBusAdapter
{
public:
    SZBusAdapter(const std::string& sn);
    virtual ~SZBusAdapter();

public:
    void SetProducer(RdkafkaProducer* producer, const std::string& topic);
    void SetFileWriter(FileWriter* fileWriter);
    int StartServer(const std::string& ip, uint16_t port);
    int StartClient(const std::string& ip, uint16_t port);

private:
    static int RecvCB(int sessionId, const char* buf, int len, void* userdata)
    {
        SZBusAdapter* that = reinterpret_cast<SZBusAdapter*>(userdata);
        return that->OnRecv2(sessionId, buf, len);
    }
    int OnRecv(int sessionId, const char* buf, int len);
    int OnRecv2(int sessionId, const char* buf, int len);

private:
    int m_hServer;
    int m_hClient;
    std::string m_topic;
    RdkafkaProducer* m_producer;
    FileWriter* m_fileWriter;
};

#endif /* SZBUSADAPTER_H_ */
