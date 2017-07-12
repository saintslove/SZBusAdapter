/*
 * ConfigDefine.h
 *
 *  Created on: 2016年12月7日
 *      Author: wong
 */

#ifndef CONFIGDEFINE_H_
#define CONFIGDEFINE_H_

// 配置文件的路径
#define CONFIG_PATH "./SZBusAdapter.conf"

// 配置文件中用到的key值定义
#define LOG_LEVEL   "log_level"     // 日志级别
#define LOG_PATH    "log_path"      // 日志路径
#define DEVSN       "devsn"         // 设备SN
#define LISTEN_IP   "listen_ip"     // 本地监听IP
#define LISTEN_PORT "listen_port"   // 本地监听端口
#define PUSHER_IP   "pusher_ip"     // 转发服务器的IP
#define PUSHER_PORT "pusher_port"   // 转发服务器的端口
#define BACKUP_PATH "backup_path"   // 数据备份目录
#define KAFKA_VERSION "kafka_version" // kafka版本号
#define KAFKA_BROKERS "kafka_brokers" // kafka broker列表，形如"ip1:port1,ip2:port2,..."
#define KAFKA_TOPIC   "kafka_topic"   // kafka topic

#endif /* CONFIGDEFINE_H_ */
