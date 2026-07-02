import json
import logging
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import RegexMaskingInstruction

logging.getLogger("drain3").setLevel(logging.ERROR)

# 仅包含 loginfo 的正则，不再带有头部拼接信息的正则
masking_instructions = [
    RegexMaskingInstruction(r"\{.*\}", "<JSON>"),
    RegexMaskingInstruction(r"\[[a-zA-Z0-9\-]+\]", "<THREAD>"),
    RegexMaskingInstruction(r"\([\w]+\.java:\d+\)", "<CODE_LINE>"),
    RegexMaskingInstruction(r"\b[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}\b", "<UUID>"),
    RegexMaskingInstruction(r"\b0x[a-fA-F0-9]+\b", "<HEX>"),
    RegexMaskingInstruction(r"\b[a-fA-F0-9]{10,}\b", "<HEX>"),
    RegexMaskingInstruction(r"\b\d+\b", "<NUM>")
]

def test_drain3(depth, sim_th):
    config = TemplateMinerConfig()
    config.drain_depth = depth
    config.drain_sim_th = sim_th
    config.masking_instructions = masking_instructions
    
    miner = TemplateMiner(None, config=config)
    
    total_logs = 0
    with open("output_filtered.json", "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                loginfo = data.get("loginfo", "")
                if loginfo:
                    miner.add_log_message(loginfo)
                    total_logs += 1
            except:
                pass
                
    clusters = miner.drain.clusters
    templates = []
    for c in sorted(clusters, key=lambda x: x.size, reverse=True):
        templates.append((c.size, c.get_template()))
        
    return total_logs, len(clusters), templates

depths = [4, 6, 8, 10]
sim_ths = [0.4, 0.7, 0.8, 0.85, 0.9]

print(f"{'Depth':<6} | {'Sim_th':<8} | {'Clusters':<8} | {'Top Templates'}")
print("-" * 120)

for d in depths:
    for s in sim_ths:
        total, num_clusters, templates = test_drain3(d, s)
        
        # 只打印前3个模板的概览，以及是否分开了 Request/Response
        # 看看是否存在 Request=<JSON> 和 Response=<JSON> 被分开的迹象
        separated = False
        request_count = 0
        response_count = 0
        overfitted_count = 0
        for size, tmpl in templates:
            if "Request=<JSON>" in tmpl:
                request_count += 1
            elif "Response=<JSON>" in tmpl:
                response_count += 1
            elif "queryFix <*>" in tmpl or "queryFix Response=<JSON> <*>" in tmpl:
                overfitted_count += 1
                
        status = []
        if request_count > 0 and response_count > 0:
            status.append(f"Separated(Req:{request_count}, Resp:{response_count})")
        if overfitted_count > 0:
            status.append(f"Overfitted({overfitted_count})")
            
        status_str = ", ".join(status) if status else "Other"
        
        print(f"{d:<6} | {s:<8} | {num_clusters:<8} | {status_str}")

# 详细打印几个关键配置的结果
print("\n" + "="*80)
print("详细探索最佳配置: Depth=8, Sim_th=0.85")
_, num, tmpls = test_drain3(8, 0.85)
for size, t in tmpls:
    print(f"[{size:4d}] {t}")
    
print("\n详细探索最佳配置: Depth=10, Sim_th=0.9")
_, num, tmpls = test_drain3(10, 0.9)
for size, t in tmpls:
    print(f"[{size:4d}] {t}")
