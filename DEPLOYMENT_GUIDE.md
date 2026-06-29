# 大掌柜日志分析服务 (Python) - 部署与测试指南

恢复命令：agy --conversation=396b9e0b-dd73-41c0-937a-3831ae74df21

使用pex进行打包。在pycharm的终端打包即可。打包命令为：pex --sources-directory . -r requirements.txt -e main:main --python=python3.11 --platform=manylinux2014_x86_64-cp-311-cp311 -o log_analyzer.pex
log_analyzer.pex。打完包后，copy到linux上，使用 python log_analyzer.pex命令即可运行。

本文档包含如何将该 Python 日志分析引擎转移到 Linux 环境下进行部署、配置、启动以及接口测试的完整流程。

## 1. 转移与解压

将包含所有 Python 代码的 `log_analyzer` 文件夹拷贝到目标 Linux 服务器中（例如放置于 `/opt/log_analyzer`）。

目录内必须包含以下文件：
- `config.py`
- `log_template_engine.py`
- `kafka_trainer.py`
- `api.py`
- `main.py`
- `requirements.txt`

## 2. 环境准备

建议在 Linux 环境中使用 Python 3.8+ 版本，并通过虚拟环境（virtualenv）隔离依赖：

```bash
# 1. 进入代码目录
cd /opt/log_analyzer

# 2. 创建名为 venv 的 Python 虚拟环境
python3 -m venv venv

# 3. 激活虚拟环境
source venv/bin/activate

# 4. 安装所需依赖库
pip install -r requirements.txt
```

## 3. 修改配置文件

服务启动前，必须修改 `config.py` 中的占位符信息，使其指向真实的内部组件：

1. 使用 vim 或 nano 编辑 `config.py`。
2. 找到 `KAFKA_CONFIG` 字典。
3. 将 `<KAFKA_BROKER_IP_1>` 到 `<KAFKA_BROKER_IP_5>` 替换为贵司真实 Kafka 集群的 IP 地址。
4. 确认 `topic` 名称是否为实际生产的日志 Topic。
5. （可选）你可以修改 `HTTP_CONFIG` 中的 `port` 来更改 FastAPI 的监听端口（默认 8000）。

*注：Drain3 的持久化文件 `drain3_state.bin` 将会自动生成并保存在该同级目录下，请确保该目录具有写入权限。*

## 4. 启动服务

**方法一：前台运行（用于初步调试查看日志）**
```bash
python main.py
```
此时你可以直接在终端中看到 Uvicorn 的启动日志和 Kafka 的连接状态。按 `Ctrl+C` 可以平滑退出。

**方法二：后台持续运行（生产环境推荐）**
使用 `nohup` 放入后台，并将所有的标准输出与错误流重定向到日志文件中：
```bash
nohup python main.py > log_analyzer.log 2>&1 &
```
启动后可以使用 `tail -f log_analyzer.log` 动态查看服务日志，以确认 Kafka 是否在正常消费数据。

## 5. 接口测试验证

服务成功启动后，将在本机的配置端口（默认 8000）开放 HTTP RESTful API，可以使用以下 `curl` 命令进行验证测试。

### 5.1 健康检查与模板统计

检查服务是否存活，并查看目前引擎从 Kafka 历史数据中训练/加载了多少个模板。

```bash
curl http://127.0.0.1:8000/health
```

**预期返回:**
```json
{"status": "ok", "total_templates": 125}
```

### 5.2 增量日志匹配测试

模拟 PM 项目向该服务发送一条日志进行匹配查询。

```bash
curl -X POST http://127.0.0.1:8000/api/match_log_template \
     -H "Content-Type: application/json" \
     -d '{
           "raw_log": "2026-06-23 10:05:32 [ERROR] User 1381234 login failed", 
           "timestamp": 1750665932000
         }'
```

**预期返回 (首次发现新模板):**
```json
{
  "template_id": "a5b82c3... (MD5字符串)",
  "is_new": true,
  "template_content": "<DATE> <TIME> [<LEVEL>] User <NUM> login failed"
}
```
*注：由于是 `is_new: true`，此时服务端会自动触发 `FilePersistenceHandler` 将状态追加保存到 `drain3_state.bin` 中。*

**预期返回 (再次发送相同/类似结构日志):**
```json
{
  "template_id": "a5b82c3... (相同的MD5)",
  "is_new": false,
  "template_content": "<DATE> <TIME> [<LEVEL>] User <NUM> login failed"
}
```
