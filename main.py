# -*- coding: utf-8 -*-
"""
服务启动入口

职责：
  1. 初始化 LogTemplateEngine（触发本地持久化文件的状态恢复）
  2. 启动 Kafka 训练后台线程
  3. 启动 FastAPI HTTP 服务
  4. 注册平滑关闭逻辑
"""

import logging
import signal
import sys
import threading

import uvicorn

import api
from config import HTTP_CONFIG, KAFKA_CONFIG
from kafka_trainer import KafkaTrainer
from log_template_engine import LogTemplateEngine

# 配置基础日志格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(name)s: %(message)s"
)
logger = logging.getLogger("main")

# 屏蔽 kafka-python 底层因为 CPU 抢占造成的网络轮询超时警告 (>= ERROR 才打印)
logging.getLogger("kafka").setLevel(logging.ERROR)

# 全局变量
engine = None
kafka_trainer = None

def graceful_shutdown(signum, frame):
    """优雅关闭钩子"""
    logger.info("收到关闭信号 %s，正在平滑退出服务...", signum)
    
    # 停止 Kafka 线程
    if kafka_trainer:
        kafka_trainer.stop()
        
    # Uvicorn 内部会有自己的 shutdown 逻辑
    logger.info("服务关闭流程完成，即将退出进程。")
    sys.exit(0)

def main():
    global engine, kafka_trainer
    
    # 注册信号处理
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    logger.info("=== 正在启动大掌柜日志模板分析服务 ===")

    # 1. 初始化核心引擎（包含 F3 的状态恢复）
    try:
        engine = LogTemplateEngine()
        # 注入到 api 模块中
        api.engine_instance = engine
    except Exception as e:
        logger.error("核心引擎初始化失败: %s", e, exc_info=True)
        sys.exit(1)

    # 2. 启动 Kafka 异步训练线程（F2）
    if KAFKA_CONFIG.get("enable_consumer", True):
        try:
            kafka_trainer = KafkaTrainer(engine=engine)
            kafka_trainer.start()
        except Exception as e:
            logger.error("Kafka 训练线程启动失败: %s", e, exc_info=True)
            sys.exit(1)
    else:
        logger.info("Kafka 订阅训练已关闭 (ENABLE_KAFKA_CONSUMER=False)，当前处于纯被动接口模式。")

    # 3. 启动 HTTP API 服务（F5）
    logger.info("启动 FastAPI HTTP 服务, 监听 %s:%d", HTTP_CONFIG["host"], HTTP_CONFIG["port"])
    try:
        uvicorn.run(
            api.app, 
            host=HTTP_CONFIG["host"], 
            port=HTTP_CONFIG["port"],
            log_level="info"
        )
    except Exception as e:
        logger.error("FastAPI 服务异常退出: %s", e, exc_info=True)
    finally:
        # 当 uvicorn 退出时，确保其他资源也被释放
        if kafka_trainer:
            kafka_trainer.stop()

if __name__ == "__main__":
    main()
