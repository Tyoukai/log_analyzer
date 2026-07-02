# 大掌柜日志分析服务 - API 接口文档

本文档为 Process-Management (PM) 项目组调用 Python 日志分析服务提供标准化接口说明。

## 1. 基础信息

- **服务名称**：日志模板分析引擎
- **协议**：HTTP RESTful API
- **默认端口**：`8000`（可通过配置修改）
- **数据格式**：`application/json`

---

## 2. 日志模板匹配接口

**接口描述**：接收 PM 项目发来的一条原始日志，在内存解析树中进行匹配。如果命中已有模板，返回模板 ID；如果未命中（新模板），提取新模板并实时更新本地文件，返回新的模板 ID。

**请求路径**：`POST /api/match_log_template`

### 2.1 请求参数 (Request Body)

| 字段名 | 类型 | 必填 | 描述 |
|--------|------|------|------|
| `raw_log` | String | 是 | FileBeat 采集上报的**原始 JSON 格式日志文本（字符串化）**。 |
| `timestamp` | Long | 是 | PM 接收到该日志的时间戳（毫秒级）。主要用于防重放或扩展。 |

> **⚠️ 关于 `raw_log` 的核心约束：**
> 传递的 `raw_log` 字符串解析为 JSON 后，**必须包含**以下 5 个核心要素，且必须符合条件，否则接口将直接返回 400 报错拦截，不再进行任何引擎匹配训练：
> 1. `log_level`: 必须存在，且**必须为 `"ERROR"`**。如果是其他级别（如 INFO/WARN），引擎会报错拒绝。
> 2. `loginfo`: 日志的真实文本内容。
> 3. `source_ip`: 日志产生节点的源 IP。
> 4. `timestamp`: 日志产生时间（FileBeat 原始采集时间）。
> 5. `log` -> `file` -> `path`: 日志文件所在绝对路径。

**请求报文示例**：
```json
{
  "raw_log": "{\"loginfo\":\"[http-8080-1] (EsbUtil.java:157) NullPointerException\", \"log_level\":\"ERROR\", \"source_ip\":\"172.19.35.4\", \"timestamp\":\"2026-06-24 14:23:55,560\", \"log\":{\"file\":{\"path\":\"/home/ygt/catalina.out\"}}}",
  "timestamp": 1750665932000
}
```

### 2.2 响应参数 (Response Body)

| 字段名 | 类型 | 描述 |
|--------|------|------|
| `template_id` | String | 匹配到或新生成的模板唯一 ID（基于模板内容的 MD5 字符串）。 |
| `is_new` | Boolean | `true` 表示这是引擎从未见过的新模板；`false` 表示命中已有模板库。 |
| `template_content` | String | 生成的静态模板内容（如：`<TIMESTAMP> <LEVEL> <IP> <PATH> <THREAD> NullPointerException`）。供 PM 端做日志展示或排查使用。 |

**成功响应示例 (HTTP 200)**：
```json
{
  "template_id": "a5b82c3d4e5f6g7h8i9j0k1l2m3n4o5p",
  "is_new": true,
  "template_content": "[<TIMESTAMP>] [<LEVEL>] [<IP>] [<PATH>] <THREAD> NullPointerException"
}
```

### 2.3 错误码说明

| HTTP 状态码 | 报错详情 (`detail`) | 处理建议 |
|------------|--------------------|----------|
| **400** | 传入的日志不是有效的 JSON 格式 | 检查传入的 `raw_log` 字符串是否为合法 JSON。 |
| **400** | 缺乏日志要素: 缺少 log_level 字段 | 确认 FileBeat 上报的 JSON 中包含日志级别。 |
| **400** | 日志级别不符合要求: 当前引擎仅支持... | 过滤调用来源，确认 PM 只将 ERROR 级别的日志投递给本接口。 |
| **400** | 缺乏日志要素: 缺少以下必须字段 loginfo, source_ip... | 补全业务日志的 JSON 字段再进行请求，这通常是因为上报格式变更导致的。 |
| **500** | 匹配过程中发生内部错误 | 引擎内部处理崩溃（如读写锁冲突、内存耗尽），需检查 Python 服务端日志。 |
| **503** | LogTemplateEngine 尚未初始化完成 | 服务刚启动正在加载本地千万级模板，需稍后重试。 |

---

## 3. 服务健康检查接口

**接口描述**：用于负载均衡探测或 PM 服务启动前检查 Python 引擎是否存活。

**请求路径**：`GET /health`

**响应示例**：
```json
{
  "status": "ok",
  "total_templates": 15234
}
```
*`total_templates` 表示当前引擎在内存中维护的有效日志模板数量。*

---

## 4. 实时模板监控观测接口

**接口描述**：【预训练阶段监控专用】用于在服务运行时（如初期连接 Kafka 消费大量数据时），实时观测当前的日志模板收敛情况。通过观察长尾模板，判断是否出现“模板爆炸”以及评估当前正则表达式的拦截有效性。

**请求路径**：`GET /api/templates?limit={limit}`

### 4.1 请求参数 (Query Parameters)

| 字段名 | 类型  | 必填 | 默认值 | 描述                                       |
| ------ | ----- | ---- | ------ | ------------------------------------------ |
| `limit`| int   | 否   | 100    | 指定返回模板列表的最大条数。按命中次数倒序。|

### 4.2 响应示例

```json
{
  "total_clusters": 12,
  "showing": 2,
  "templates": [
    {
      "cluster_id": 1,
      "size": 5230,
      "template": "<THREAD> <CODE_LINE> com.apex.ygt.util.EsbUtil - queryFix Response=<JSON>"
    },
    {
      "cluster_id": 2,
      "size": 1,
      "template": "<THREAD>[esb.adapter.hst2.plugin.param.Hst2PassPlugin]java.lang.NegativeArraySizeException:null"
    }
  ]
}
```

* `total_clusters`: 当前引擎在内存中维护的模板总数。
* `size`: 某个特定模板被历史日志命中的次数。
* `template`: 被引擎正则表达式掩盖静态化之后的模板字符串。

> **💡 观测与调优建议：**
> 1. 重点观察 `total_clusters` 是否异常疯长（比如一直处于几万以上）。
> 2. 定期关注那些 `size` 为 `1`（仅出现过一次）的末尾长尾模板。如果它们的 `template` 中包含未经掩码的随机乱码、杂凑哈希、随机的交易流水号等无规则变动串，这就说明现存的正则表达式不足以拦截该动态数据。
> 3. 此时只需在 Python 代码 `config.masking_instructions` 中补充针对该“漏网之鱼”的正则表达式即可。
