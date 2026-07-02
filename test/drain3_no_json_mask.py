import json
import logging
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import RegexMaskingInstruction

logging.getLogger("drain3").setLevel(logging.ERROR)

masking_instructions = [
    RegexMaskingInstruction(r"\[[a-zA-Z0-9\-]+\]", "<THREAD>"),
    RegexMaskingInstruction(r"\([\w]+\.java:\d+\)", "<CODE_LINE>"),
    RegexMaskingInstruction(r"\b[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}\b", "<UUID>"),
    RegexMaskingInstruction(r"\b0x[a-fA-F0-9]+\b", "<HEX>"),
    RegexMaskingInstruction(r"\b[a-fA-F0-9]{10,}\b", "<HEX>"),
    RegexMaskingInstruction(r"\b\d+\b", "<NUM>")
]

config = TemplateMinerConfig()
config.drain_depth = 8
config.drain_sim_th = 0.85
config.masking_instructions = masking_instructions

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

clusters = miner.drain.clusters
print(f"Total clusters without JSON mask: {len(clusters)}")
for c in sorted(clusters, key=lambda x: x.size, reverse=True)[:10]:
    print(f"[{c.size:4d}] {c.get_template()[:150]}...")
