# -*- coding: utf-8 -*-
"""
核心引擎 - 封装 Drain3 日志模板训练与匹配的全部逻辑

职责：
  F1 - 日志预清洗（正则表达式掩码）
  F3 - 模板本地持久化（FilePersistenceHandler）
  F4 - 被动接受 PM 查询（模板匹配 / 新模板发现与增量保存）
"""

import hashlib
import json
import logging
import os
import threading

from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.masking import RegexMaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig

from config import PERSISTENCE_CONFIG

logger = logging.getLogger(__name__)


class LogTemplateEngine:
    """日志模板训练与匹配引擎

    - 初始化时自动从本地文件恢复已有模板状态（如果存在）
    - 提供 match_template() 方法供 HTTP 接口和 Kafka 训练调用
    - 线程安全：内部使用锁保护 Drain3 的读写操作
    """

    def __init__(self):
        # 线程锁，保护 Drain3 解析树的并发读写
        self._lock = threading.Lock()

        # ------------------------------------------------------------------
        # F3 - 初始化 FilePersistenceHandler（持久化处理器）
        # 服务启动时自动检测本地是否存在已训练好的模板状态文件
        # 如果存在，TemplateMiner 会自动从文件反序列化恢复完整的解析树状态
        # ------------------------------------------------------------------
        state_file = PERSISTENCE_CONFIG["state_file"]
        self._persistence_handler = FilePersistence(state_file)

        if os.path.exists(state_file):
            logger.info("检测到已有模板状态文件 [%s]，将恢复历史训练成果", state_file)
        else:
            logger.info("未检测到模板状态文件，将从零开始训练")

        # ------------------------------------------------------------------
        # F1 - 配置正则表达式掩码规则
        # 鉴于大掌柜系统日志格式极不规范，必须在聚类之前进行高强度掩码处理
        # 参照 logparser_demo.py 中的规则
        # ------------------------------------------------------------------
        config = TemplateMinerConfig()
        config.masking_instructions = [
            # 1. 替换嵌套的 JSON 数据内容（如 Request={...}）
            # 注：不使用 (?s) 跨行匹配，确保能够分别掩盖同行内或被静态信息分隔的多个 JSON 块
            RegexMaskingInstruction(r"\{.*\}", "<JSON>"),
            
            # 2. 替换重组后的时间戳头部 (e.g. [2026-06-24 14:23:55,560])
            RegexMaskingInstruction(r"^\[\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,\.]?\d*\]", "<TIMESTAMP>"),
            
            # 3. 替换重组后的日志级别 (e.g. [ERROR])
            RegexMaskingInstruction(r"\[(ERROR|WARN|INFO|DEBUG|CRITICAL|FATAL|TRACE)\]", "<LEVEL>"),
            
            # 4. 替换 IP 地址（包括带方括号和不带方括号的孤立 IP）
            RegexMaskingInstruction(r"\[?\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b\]?", "<IP>"),
            
            # 5. 替换重组后的文件路径 (e.g. [/home/ygt/LiveBOS_Tomcat/logs/catalina.out])
            RegexMaskingInstruction(r"\[/.*?\]", "<PATH>"),
            
            # 以下针对 loginfo 内部的动态变量进行掩码
            # 6. 替换线程名或特定适配器标识 (如 [http-8080-1] 或 [CJOIIS])
            RegexMaskingInstruction(r"\[[a-zA-Z0-9\-]+\]", "<THREAD>"),
            
            # 7. 替换 Java 类及行号 (如 (EsbUtil.java:157))
            RegexMaskingInstruction(r"\([\w]+\.java:\d+\)", "<CODE_LINE>"),
            
            # 8. 替换标准 UUID
            RegexMaskingInstruction(r"\b[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}\b", "<UUID>"),
            
            # 9. 替换十六进制长字符串 (如 0x开头的内存地址，或长于10位的纯杂凑哈希、MAC地址等)
            RegexMaskingInstruction(r"\b0x[a-fA-F0-9]+\b", "<HEX>"),
            RegexMaskingInstruction(r"\b[a-fA-F0-9]{10,}\b", "<HEX>"),
            
            # 10. 兜底替换所有剩余的独立数字 (必须放最后)
            RegexMaskingInstruction(r"\b\d+\b", "<NUM>"),
        ]

        # ------------------------------------------------------------------
        # 初始化 TemplateMiner
        # persistence_handler 会自动检测并加载已有的本地状态文件
        # ------------------------------------------------------------------
        self._template_miner = TemplateMiner(
            persistence_handler=self._persistence_handler, config=config
        )

        template_count = len(self._template_miner.drain.clusters)
        logger.info(
            "LogTemplateEngine 初始化完成，当前已有模板数量: %d", template_count
        )

    def preprocess_log(self, raw_log: str) -> str:
        """解析 JSON 日志，校验必填要素并重组文本"""
        try:
            log_dict = json.loads(raw_log)
        except json.JSONDecodeError:
            raise ValueError("传入的日志不是有效的 JSON 格式")

        # 1. 过滤：目前只关心 ERROR 级别的日志
        log_level = log_dict.get("log_level")
        if not log_level:
            raise ValueError("缺乏日志要素: 缺少 log_level 字段")
            
        if log_level != "ERROR":
            raise ValueError(f"日志级别不符合要求: 当前引擎仅支持处理 ERROR 级别的日志，收到的是 {log_level}")
            
        # 2. 提取核心关心的字段
        loginfo = log_dict.get("loginfo")
        source_ip = log_dict.get("source_ip")
        timestamp = log_dict.get("timestamp")
        
        # 提取嵌套的 path
        path = None
        log_obj = log_dict.get("log")
        if isinstance(log_obj, dict):
            file_obj = log_obj.get("file")
            if isinstance(file_obj, dict):
                path = file_obj.get("path")
                
        # 校验必备要素
        missing_fields = []
        if not loginfo: missing_fields.append("loginfo")
        if not source_ip: missing_fields.append("source_ip")
        if not timestamp: missing_fields.append("timestamp")
        if not path: missing_fields.append("log.file.path")
        
        if missing_fields:
            raise ValueError(f"缺乏日志要素: 缺少以下必须字段 {', '.join(missing_fields)}")
                
        # 3. 将其余所有无用信息丢弃，重组为干净的结构化纯文本
        # 格式如：[2026-06-24 14:23:55,560] [ERROR] [172.19.35.4] [/home/ygt/catalina.out] [http-8080-1] ...
        clean_log = f"[{timestamp}] [{log_level}] [{source_ip}] [{path}] {loginfo}"
        return clean_log

    def match_template(self, raw_log: str, timestamp: int) -> dict:
        """对一条原始日志进行模板匹配/提取

        对应功能 F4：
          - 命中已有模板 → 返回 template_id + is_new=False
          - 发现新模板   → 更新内存解析树 + 触发 FilePersistenceHandler
                           更新本地持久化文件 + 返回 template_id + is_new=True

        Args:
            raw_log:   原始日志文本 (支持 JSON)
            timestamp: 日志产生的时间戳（毫秒级）

        Returns:
            dict: {
                "template_id":      str,   # 模板内容的 MD5 哈希
                "is_new":           bool,  # 是否为新发现的模板
                "template_content": str,   # 静态模板内容
            }
        """
        # 预处理与强校验
        processed_log = self.preprocess_log(raw_log)

        with self._lock:
            # Drain3 的 add_log_message 同时完成"匹配 + 训练"：
            #   - 如果匹配到已有模板，返回该模板信息
            #   - 如果未匹配，自动提取新模板，更新解析树，
            #     并通过 FilePersistenceHandler 将最新状态同步到本地文件
            result = self._template_miner.add_log_message(processed_log)

        # 判断是否为新模板
        change_type = result.get("change_type")
        is_new = change_type == "cluster_created"

        # 获取模板内容
        cluster = result["cluster_id"]
        template_mined = result["template_mined"]

        # 使用 MD5 哈希生成确定性的 template_id
        template_id = hashlib.md5(template_mined.encode("utf-8")).hexdigest()

        if is_new:
            logger.info(
                "发现新模板 | template_id=%s | content=%s",
                template_id,
                template_mined,
            )

        return {
            "template_id": template_id,
            "is_new": is_new,
            "template_content": template_mined,
        }

    def get_template_count(self) -> int:
        """获取当前模板总数"""
        return len(self._template_miner.drain.clusters)
