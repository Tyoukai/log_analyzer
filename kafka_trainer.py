# -*- coding: utf-8 -*-
"""
Kafka 异步训练线程

职责 (F2)：
  - 作为一个 Kafka Consumer Group
  - 异步订阅大掌柜日志 Topic，持续拉取日志数据
  - 每拉取一条日志，就调用 LogTemplateEngine 进行聚类训练
"""

import json
import logging
import threading
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import KAFKA_CONFIG
from log_template_engine import LogTemplateEngine

logger = logging.getLogger(__name__)


class KafkaTrainer(threading.Thread):
    def __init__(self, engine: LogTemplateEngine):
        super().__init__(name="KafkaTrainerThread", daemon=True)
        self.engine = engine
        self._stop_event = threading.Event()

    def run(self):
        logger.info("Kafka 训练线程启动中，准备连接 Kafka 集群: %s", KAFKA_CONFIG["bootstrap_servers"])
        consumer = None

        while not self._stop_event.is_set():
            try:
                # 初始化 Consumer
                if consumer is None:
                    consumer = KafkaConsumer(
                        KAFKA_CONFIG["topic"],
                        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
                        group_id=KAFKA_CONFIG["group_id"],
                        auto_offset_reset=KAFKA_CONFIG["auto_offset_reset"],
                        enable_auto_commit=True,
                        # 如果需要解析 JSON 格式的消息，可以开启下面这行
                        # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                        value_deserializer=lambda x: x.decode('utf-8', errors='ignore')
                    )
                    logger.info("成功连接到 Kafka, 开始监听 Topic: %s", KAFKA_CONFIG["topic"])

                # 消费消息
                for msg in consumer:
                    if self._stop_event.is_set():
                        break
                    
                    # 假设从 Kafka 拉取的每一条消息就是一条纯文本形式的 raw_log
                    # 如果贵司发往 Kafka 的日志是 JSON 格式，请在这里进行字段提取
                    # 例如: raw_log = msg.value.get("message", "")
                    raw_log = msg.value
                    
                    # 使用当前毫秒时间戳作为兜底。如果有真实的日志产生时间，最好从消息中提取
                    timestamp = int(time.time() * 1000)

                    if raw_log and isinstance(raw_log, str):
                        # 传入引擎进行训练
                        # add_log_message 本身即包含匹配和新模板提取，符合 F2 职责
                        try:
                            self.engine.match_template(raw_log, timestamp)
                        except ValueError as ve:
                            # 格式校验不通过的日志直接丢弃，不中断消费
                            pass
                        
            except KafkaError as e:
                logger.error("Kafka 连接或消费过程中发生错误: %s, 5秒后重试", e)
                consumer = None # 触发重新连接
                time.sleep(5)
            except Exception as e:
                logger.error("KafkaTrainer 发生未知异常: %s", e, exc_info=True)
                time.sleep(5)
                
        if consumer:
            consumer.close()
        logger.info("Kafka 训练线程已安全停止")

    def stop(self):
        """优雅关闭线程"""
        logger.info("正在请求停止 Kafka 训练线程...")
        self._stop_event.set()
