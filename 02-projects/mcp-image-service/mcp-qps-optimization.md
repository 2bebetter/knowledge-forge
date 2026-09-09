---
title: MCP 云函数生图服务 QPS 优化
date: 2026-09-09
type: technical
tags: [性能优化, 阿里云FC, MCP, Python, asyncio]
status: completed
project: mcp-image-service
difficulty: hard
publish: true
---

# MCP 云函数生图服务 QPS 优化

## 背景

百炼智能体（陈露）通过 MCP 协议调用阿里云函数计算（FC）上的生图服务，遇到并发瓶颈：

- **目标**：100 QPS
- **现状**：20 并发时请求耗时 160 秒，100 并发直接返回 429
- **架构**：用户 → 百炼智能体 → MCP 云函数 → fr.myters.cn 网关 → Gemini 模型 → OSS

## 排查过程

### 阶段 1：错误假设 - AFC 并发限制

**假设**：Google GenAI SDK 的 AFC（Automatic Function Calling）限制了并发为 10

**尝试**：
```python
# 修改 SDK 内部属性
_vertexai_client._api_client._max_remote_calls = 100
_vertexai_client._api_client._concurrency_semaphore = threading.Semaphore(100)
```

**结果**：❌ 无效

**原因**：
- AFC 是 **Automatic Function Calling**（自动函数调用循环次数），不是并发限制
- SDK v2.21.0 中 `_api_client._max_remote_calls` 属性根本不存在
- 日志 `AFC is enabled with max remote calls: 10` 表示模型自动调用函数的循环上限，与 QPS 无关

**教训**：
- 不要看到 "max remote calls: 10" 就以为是并发限制
- 先查 SDK 源码确认属性是否存在

### 阶段 2：错误假设 - fr 网关限流

**假设**：`fr.myters.cn` 网关对 `gemini-3.1-flash-image` 模型限流

**验证**：用户确认 fr 网关无 429 日志

**结论**：❌ 排除

### 阶段 3：错误假设 - 百炼模型限流

**假设**：阿里云百炼模型服务 QPS 配额为 10

**验证**：用户澄清 `gemini-3.1-flash-image` 不是百炼提供的，是 fr 网关提供的

**结论**：❌ 排除

### 阶段 4：正确发现 - 模型推理时间

**测试**：
```bash
# 1 并发 10 轮测试
python3 test_mcp_concurrency.py <URL> <TOKEN> 1 10
```

**结果**：
```
单请求延迟 P50: 15.77s
```

**发现**：
- 单次请求本身就要 12-16 秒
- 这是模型推理 + OSS 上传的时间，无法优化
- 瓶颈不在代码，在外部服务

**关键计算**：
```
单实例并发：200
单请求响应：15 秒
理论 QPS：200 / 15 ≈ 13 QPS

要 100 QPS：
需要 100 × 15 = 1500 并发
1500 / 200 = 8 个实例
```

### 阶段 5：代码问题 - 同步阻塞事件循环

**问题**：
```python
# 问题代码
@mcp.tool()
async def gen_simple_image(prompt: str, aspect_ratio: str = "16:9") -> dict:
    tool = GenImageTool()
    result = tool.gen_image(...)  # 同步调用，阻塞事件循环
    return result
```

**影响**：
- `async def` 里调用同步代码，会阻塞整个事件循环
- 20 个并发请求变成串行处理
- 延迟从 15s 膨胀到 160s

**修复**：
```python
# 修复后
@mcp.tool()
async def gen_simple_image(prompt: str, aspect_ratio: str = "16:9") -> dict:
    tool = GenImageTool()
    result = await asyncio.to_thread(tool.gen_image, prompt, aspect_ratio, image_model=GEMINI_SIMPLE_IMAGE_MODEL)
    return result
```

**效果**：
- 同步调用扔到线程池，不阻塞事件循环
- 20 并发可以真正并行处理

### 阶段 6：FC 配额问题

**现象**：100 并发全部返回 429
```
Code: ResourceExhausted
Message: Function concurrent request count exceeded
```

**分析**：
- FC 有三层并发限制：单实例并发度、函数级并发度、地域配额
- 单实例并发度：200（已配置）
- 函数级并发度：未知（可能很低）
- 地域配额：剩余 2 个实例

**解决方案**：
1. 申请 FC 实例配额提升到 8 个以上
2. 配置弹性伸缩策略

### 阶段 7：弹性伸缩配置

**配置**：
```
最小实例数：1（固定值）
最大实例数：5
扩容策略：按水位/并发数
```

**测试结果**：
```
100 并发：
- 成功：100/100
- 总耗时：89.71s
- P50 延迟：48.86s
- 有效 QPS：1.11
```

**分析**：
- 延迟增加 3 倍（15s → 48s）
- 可能原因：冷启动、上游限流、实例资源竞争

### 阶段 8：实例不释放问题

**现象**：请求结束后 20 分钟，实例仍保持 5 个，CPU 0.533

**原因 1：弹性策略配置错误**
```
错误配置：最小实例数动态 1-5
结果：水位升高后，最小实例数被提升到 5，永远不会缩容
```

**修复**：
```
正确配置：最小实例数固定 1
```

**原因 2：会话超时过长**
```
会话有效期：21600 秒 = 6 小时
空闲超时：1800 秒 = 30 分钟
```

**修复**：
```
空闲超时：改为 60-120 秒
会话有效期：改为 1800-3600 秒
```

## 最终方案

### 代码改动

#### 1. MCP 服务器入口（关键修复）

```python
import asyncio
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(name="python-sse-generate-image", stateless_http=False)

@mcp.tool()
async def gen_simple_image(prompt: str, aspect_ratio: str = "16:9") -> dict:
    """AI简单绘画服务"""
    tool = GenImageTool()
    result = await asyncio.to_thread(tool.gen_image, prompt, aspect_ratio, image_model=GEMINI_SIMPLE_IMAGE_MODEL)
    return result

# 其他 6 个工具同理...
```

#### 2. gen_image_tool.py（回退无效改动）

```python
def get_vertexai_client():
    """获取全局 VertexAI Client 实例"""
    global _vertexai_client
    if _vertexai_client is None:
        with _vertexai_client_lock:
            if _vertexai_client is None:
                _vertexai_client = genai.Client(
                    vertexai=False,
                    http_options={
                        'base_url': os.getenv('VERTEX_AI_BASE_URL'),
                        'api_version': 'v1beta',
                        'timeout': 300000,
                    },
                    api_key=os.getenv('VERTEX_AI_API_KEY')
                )
    return _vertexai_client
```

### FC 配置

```
单实例并发度：200
最小实例数：1（固定值）
最大实例数：5-10
扩容策略：按水位/并发数
会话空闲超时：120 秒
会话有效期：3600 秒
```

### 理论 QPS

```
单实例：200 并发 / 15 秒 ≈ 13 QPS
5 实例：1000 并发 / 15 秒 ≈ 66 QPS
8 实例：1600 并发 / 15 秒 ≈ 106 QPS ✅
```

## 关键发现

1. **AFC 不是并发限制器**
   - AFC = Automatic Function Calling，是模型自动调用函数的循环次数
   - 日志 `max remote calls: 10` 与 QPS 无关
   - 不要看到 "max remote calls" 就以为是并发限制

2. **async 函数里的同步调用会阻塞事件循环**
   - `async def` 里调用同步代码，会阻塞整个事件循环
   - 必须用 `asyncio.to_thread` 扔到线程池
   - 这是 Python 异步编程的常见陷阱

3. **模型推理时间是硬瓶颈**
   - 15 秒响应时间是外部 API 决定的，无法优化
   - 要提升 QPS，只能增加并发实例数
   - 或者换更快的模型（如果可能）

4. **FC 弹性伸缩配置要谨慎**
   - 最小实例数要固定，不要动态调整
   - 会话超时时间要合理，避免实例长时间不释放
   - 监控实例数和 CPU 使用率，及时调整配置

5. **排查问题要有系统性**
   - 不要一上来就改代码，先确认瓶颈在哪
   - 用数据说话，不要凭感觉
   - 每次只改一个变量，观察效果

## 测试脚本

创建了 `test_mcp_concurrency.py` 用于压测：

```bash
# 1 并发测试（基线）
python3 test_mcp_concurrency.py <URL> <TOKEN> 1 1

# 50 并发测试
python3 test_mcp_concurrency.py <URL> <TOKEN> 50 50

# 100 并发测试
python3 test_mcp_concurrency.py <URL> <TOKEN> 100 100
```

脚本特点：
- 标准 MCP Streamable HTTP 协议
- 每个线程独立 session
- 实时打印进度
- 输出 P50/P95/P99 延迟和 QPS

## 成本分析

### FC 计费方式

```
费用 = vCPU 使用量 + 内存使用量 + 调用次数
```

### 优化前（错误配置）

```
最小实例数：动态 1-5
会话超时：30 分钟
结果：5 个实例常驻，即使没请求
费用：5 × 24 小时 × 单价
```

### 优化后（正确配置）

```
最小实例数：固定 1
会话超时：2 分钟
结果：平时 1 个实例，高并发临时扩容
费用：1 × 24 小时 + 偶尔的 5 实例费用
```

**预计节省**：80% 以上的成本

## 后续优化方向

### 短期

- [x] 修复 async 阻塞问题
- [x] 配置弹性伸缩
- [x] 优化会话超时
- [ ] 申请 FC 配额提升到 8 个实例
- [ ] 压测验证 100 QPS 目标

### 长期

1. **模型优化**
   - 如果 fr.myters.cn 能优化响应时间到 2 秒以内，单实例可达 100 QPS
   - 但这取决于外部服务，不可控

2. **缓存策略**
   - 对相同 prompt 的生图结果做缓存
   - 减少重复请求

3. **批量处理**
   - 支持批量生图接口
   - 减少网络往返次数

## 参考资料

- [阿里云 FC 创建 Web 函数](https://help.aliyun.com/zh/functioncompute/creating-a-web-function)
- [阿里云 FC 配置单实例并发度](https://help.aliyun.com/zh/functioncompute/configure-the-concurrency-of-a-single-instance)
- [Google GenAI SDK 文档](https://github.com/googleapis/python-genai)
- [Python asyncio.to_thread 文档](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread)
- [MCP 协议规范](https://modelcontextprotocol.io/)

## 总结

这次优化经历了很多弯路，但最终找到了根本原因：

1. **代码层面**：async 函数里的同步调用阻塞了事件循环
2. **基础设施层面**：FC 实例配额不足，弹性伸缩配置错误
3. **业务层面**：模型推理时间 15 秒是硬瓶颈，无法优化

**核心教训**：
- 不要凭感觉排查问题，用数据说话
- 每次只改一个变量，观察效果
- 先确认瓶颈在哪，再决定优化方向
- 异步编程要注意事件循环阻塞问题

最终方案：修复代码 + 配置弹性伸缩 + 申请配额提升，预计可达 100 QPS。
