import json
import logging
import re
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import RegexMaskingInstruction

logging.getLogger("drain3").setLevel(logging.ERROR)

masking_instructions = [
    # 注意，我们不需要整体替换 JSON 了，因为我们在预处理阶段替换
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

def smart_preprocess(loginfo):
    # 尝试在 loginfo 中寻找 JSON 结构 { ... }
    match = re.search(r"(\{.*\})", loginfo)
    if match:
        json_str = match.group(1)
        try:
            payload = json.loads(json_str)
            extracted = []
            if "func" in payload:
                extracted.append(f"func={payload['func']}")
            if "fldm" in payload:
                extracted.append(f"fldm={payload['fldm']}")
            if "code" in payload:
                extracted.append(f"code={payload['code']}")
            if "note" in payload:
                extracted.append(f"note={payload['note']}")
                
            if extracted:
                meta = "[" + ", ".join(extracted) + "]"
                # 用提取的元数据 + <JSON> 替换原本庞大的 json 块
                loginfo = loginfo[:match.start()] + meta + " <JSON>" + loginfo[match.end():]
            else:
                loginfo = loginfo[:match.start()] + "<JSON>" + loginfo[match.end():]
        except:
            loginfo = loginfo[:match.start()] + "<JSON>" + loginfo[match.end():]
    return loginfo

total_logs = 0
with open("output_filtered.json", "r", encoding="utf-8") as f:
    for line in f:
        try:
            data = json.loads(line.strip())
            loginfo = data.get("loginfo", "")
            if loginfo:
                processed = smart_preprocess(loginfo)
                miner.add_log_message(processed)
                total_logs += 1
        except:
            pass

clusters = miner.drain.clusters
print(f"Total clusters with smart JSON extraction: {len(clusters)}")
for c in sorted(clusters, key=lambda x: x.size, reverse=True)[:10]:
    print(f"[{c.size:4d}] {c.get_template()}")
