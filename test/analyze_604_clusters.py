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
req_clusters = []
res_clusters = []
for c in clusters:
    tmpl = c.get_template()
    if "Request=" in tmpl:
        req_clusters.append(c)
    elif "Response=" in tmpl:
        res_clusters.append(c)

print(f"Total clusters: {len(clusters)}")
print(f"Request clusters: {len(req_clusters)}")
print(f"Response clusters: {len(res_clusters)}")

print("\n--- Top 5 Request Clusters ---")
for c in sorted(req_clusters, key=lambda x: x.size, reverse=True)[:5]:
    print(f"[{c.size:4d}] {c.get_template()[:150]}...")

print("\n--- Top 5 Response Clusters ---")
for c in sorted(res_clusters, key=lambda x: x.size, reverse=True)[:5]:
    print(f"[{c.size:4d}] {c.get_template()[:150]}...")
    
print("\n--- Random Response Clusters of size 1 ---")
singletons = [c for c in res_clusters if c.size == 1]
print(f"Number of singleton Response clusters: {len(singletons)}")
for c in singletons[:5]:
    print(f"[{c.size:4d}] {c.get_template()[:150]}...")
