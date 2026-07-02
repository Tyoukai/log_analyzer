import json
import logging
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import RegexMaskingInstruction

logging.getLogger("drain3").setLevel(logging.ERROR)

def test_drain3_stripped(depth, sim_th):
    config = TemplateMinerConfig()
    config.drain_depth = depth
    config.drain_sim_th = sim_th
    
    # 剥离了固定头部后，只针对 loginfo 内部的掩码
    config.masking_instructions = [
        RegexMaskingInstruction(r"\{.*\}", "<JSON>"),
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
                    # 【核心】：完全按照方案B，不拼凑时间、IP等头部，只把纯业务 loginfo 送入引擎
                    miner.add_log_message(loginfo)
            except:
                pass
                
    clusters = miner.drain.clusters
    templates = [(c.size, c.get_template()) for c in sorted(clusters, key=lambda x: x.size, reverse=True)]
    return len(clusters), templates

print("开始以剥离头部(方案B) + 保留 <JSON> 掩码 的方式，寻找最佳参数组合...\n")
print(f"{'Depth':<6} | {'Sim_th':<8} | {'最终分类数':<10} | {'效果评估'}")
print("-" * 60)

for d in [4, 6, 8, 10]:
    for s in [0.4, 0.6, 0.8, 0.85, 0.9]:
        num_clusters, templates = test_drain3_stripped(d, s)
        
        req_count = sum(1 for size, t in templates if "Request=<JSON>" in t)
        res_count = sum(1 for size, t in templates if "Response=<JSON>" in t)
        mixed_count = sum(1 for size, t in templates if "queryFix <*>" in t)
        
        status = ""
        if mixed_count > 0:
            status = "❌ 严重过拟合 (Req和Res被混淆为 <*>)"
        elif req_count > 0 and res_count > 0:
            if num_clusters > 15:
                status = "⚠️ 欠拟合 (碎片模板过多)"
            else:
                status = "✅ 完美分离 (Req和Res独立，且噪音收敛)"
        else:
            status = "未知异常"
            
        print(f"{d:<6} | {s:<8} | {num_clusters:<10} | {status}")

print("\n\n" + "="*80)
print("🏆 最佳参数 [Depth=8, Sim_th=0.85] 下的最终模板骨架一览：")
_, tmpls = test_drain3_stripped(8, 0.85)
for size, t in tmpls:
    print(f"命中 {size:4d} 次 -> {t}")

