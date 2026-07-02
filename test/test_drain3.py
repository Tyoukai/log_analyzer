import json
import logging
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import RegexMaskingInstruction

# 关闭 drain3 内部的一些不必要的 log
logging.getLogger("drain3").setLevel(logging.ERROR)

def run_drain3(depth, sim_th):
    config = TemplateMinerConfig()
    config.drain_depth = depth
    config.drain_sim_th = sim_th
    config.masking_instructions = [
        RegexMaskingInstruction(r"\{.*\}", "<JSON>"),
        RegexMaskingInstruction(r"^\[\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,\.]?\d*\]", "<TIMESTAMP>"),
        RegexMaskingInstruction(r"\[(ERROR|WARN|INFO|DEBUG|CRITICAL|FATAL|TRACE)\]", "<LEVEL>"),
        RegexMaskingInstruction(r"\[?\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b\]?", "<IP>"),
        RegexMaskingInstruction(r"\[/.*?\]", "<PATH>"),
        RegexMaskingInstruction(r"\[[a-zA-Z0-9\-]+\]", "<THREAD>"),
        RegexMaskingInstruction(r"\([\w]+\.java:\d+\)", "<CODE_LINE>"),
        RegexMaskingInstruction(r"\b[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}\b", "<UUID>"),
        RegexMaskingInstruction(r"\b0x[a-fA-F0-9]+\b", "<HEX>"),
        RegexMaskingInstruction(r"\b[a-fA-F0-9]{10,}\b", "<HEX>"),
        RegexMaskingInstruction(r"\b\d+\b", "<NUM>")
    ]
    
    miner = TemplateMiner(None, config=config)
    with open("output_filtered.json", "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                loginfo = data.get("loginfo", "")
                if loginfo:
                    miner.add_log_message(loginfo)
            except:
                pass
    
    print(f"\n--- config: depth={depth}, sim_th={sim_th} ---")
    print(f"Total clusters: {len(miner.drain.clusters)}")
    for cluster in sorted(miner.drain.clusters, key=lambda c: c.size, reverse=True)[:5]:
        print(f"Size: {cluster.size} | Template: {cluster.get_template()}")

run_drain3(4, 0.4)
run_drain3(10, 0.5)
run_drain3(8, 0.6)
