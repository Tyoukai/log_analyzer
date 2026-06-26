# -*- coding: utf-8 -*-
"""
FastAPI HTTP 接口

职责 (F5)：
  - 向 PM 暴露 MatchLogTemplate 接口（HTTP RESTful API）
  - 接收 JSON 格式的原始日志
  - 调用引擎获取匹配结果
  - 返回 template_id 和 is_new
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# 创建 FastAPI 实例
app = FastAPI(
    title="大掌柜日志模板分析服务",
    description="提供给 PM 的内部被动日志匹配引擎，无数据库依赖",
    version="1.0.0",
)

# 依赖注入：在 main.py 中会给这个变量赋值
engine_instance = None

# =============================================================================
# 定义接口模型
# =============================================================================
class MatchLogRequest(BaseModel):
    raw_log: str = Field(..., description="原始日志文本", example="2026-06-23 10:05:32 [ERROR] User 1381234 login failed")
    timestamp: int = Field(..., description="日志产生的时间戳(毫秒级)", example=1750665932000)

class MatchLogResponse(BaseModel):
    template_id: str = Field(..., description="匹配到/新生成的模板唯一ID(MD5哈希)")
    is_new: bool = Field(..., description="是否为增量发现的新模板")
    template_content: str = Field(..., description="静态模板内容(辅助排查用)")

# =============================================================================
# 定义路由
# =============================================================================
@app.post("/api/match_log_template", response_model=MatchLogResponse)
async def match_log_template(req: MatchLogRequest):
    """
    匹配日志模板接口 (PM 调用)
    
    1. 接收一条 raw_log
    2. 送入 Drain3 内存解析树匹配
    3. 命中已有模板 -> 返回其 template_id, is_new=false
    4. 未命中 -> 提取新模板，将新模板节点追加到内存树并持久化到本地文件，返回新 template_id, is_new=true
    """
    if engine_instance is None:
        raise HTTPException(status_code=503, detail="LogTemplateEngine 尚未初始化完成")
    
    if not req.raw_log.strip():
        raise HTTPException(status_code=400, detail="raw_log 不能为空")
        
    try:
        result = engine_instance.match_template(req.raw_log, req.timestamp)
        return MatchLogResponse(
            template_id=result["template_id"],
            is_new=result["is_new"],
            template_content=result["template_content"]
        )
    except ValueError as ve:
        # PM 传递的日志缺乏模板要素或格式错误
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"匹配过程中发生内部错误: {str(e)}")

@app.get("/health")
async def health_check():
    """健康检查接口，附带返回当前模板总数"""
    if engine_instance is None:
        return {"status": "starting"}
    
    count = engine_instance.get_template_count()
    return {"status": "ok", "total_templates": count}
