/*
 * Server.cpp
 *
 *  Created on: 2017年2月17日
 *      Author: wong
 */

#include "Config.h"
#include "ConfigDefine.h"
#include "LogHelper.h"
#include "SZBusAdapter.h"
#include "RdkafkaProducer.h"
#include "FileWriter.h"

int main()
{
    CONFIG->Init(CONFIG_PATH);
    LogHelper logHelper(CFGINT(LOG_LEVEL), CFGSTR(LOG_PATH));

    RdkafkaProducer producer(CFGSTR(KAFKA_VERSION), CFGSTR(KAFKA_BROKERS));
    FileWriter fileWriter(CFGSTR(BACKUP_PATH));

    SZBusAdapter adapter(CFGSTR(DEVSN));
    adapter.SetProducer(&producer, CFGSTR(KAFKA_TOPIC));
    adapter.SetFileWriter(&fileWriter);
    adapter.StartServer(CFGSTR(LISTEN_IP), CFGINT(LISTEN_PORT));
    adapter.StartClient(CFGSTR(PUSHER_IP), CFGINT(PUSHER_PORT));

    pause();
    return 0;
}

