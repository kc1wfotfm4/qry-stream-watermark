# qry-stream-watermark

把流式数据源的窗口划分、允许迟到时长与水印推进位置记录成可查询的服务，支持按数据源查看当前水印与已触发窗口，并拒绝时间倒流的进度上报。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

## 构建、测试与启动

```bash
go build ./...
go test ./...
go run .
```

服务默认监听 `127.0.0.1:8080`。可用环境变量覆盖：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `DB_PATH` | `qry-stream-watermark.db` | SQLite 数据库文件路径 |

## 已公开的入口

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

### `POST /api/v1/streams`

登记一条数据流。请求体：

```json
{"stream_id":"orders","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}
```

`stream_id` 与 `window_type` 必须是非空字符串；`window_size_seconds` 必须大于零；`allowed_lateness_seconds` 不得为负。成功时 HTTP 201，回显服务保存的四项配置：

```json
{"stream_id":"orders","window_type":"tumbling","window_size_seconds":300,"allowed_lateness_seconds":60}
```

同一 `stream_id` 重复登记返回 HTTP 409（`stream_already_exists`），原记录保持不变。

### `POST /api/v1/streams/{stream_id}/watermarks`

提交数据流的水印（非负整数 Unix 秒）。请求体：

```json
{"watermark":1717430400}
```

成功时 HTTP 200，返回当前记录：

```json
{"stream_id":"orders","watermark":1717430400}
```

只有大于当前水印的提交会推进水印；相等提交成功且保持不变；早于当前水印返回 HTTP 409（`watermark_out_of_order`）。比较与写入在同一事务内完成，并发提交的最终值是参与者中的最高水印。

### `GET /api/v1/streams/{stream_id}/watermark`

返回数据流当前水印，字段与提交响应相同。新登记数据流的初始水印为 `0`。未登记的 `stream_id` 返回 HTTP 404（`stream_not_found`）。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。

| HTTP | code | 含义 |
|---|---|---|
| 400 | `invalid_request` | 字段缺失、类型不符、窗口长度非正或水印非法 |
| 404 | `stream_not_found` | 路径中的 `stream_id` 未登记 |
| 404 | `route_not_found` | 路径不匹配任何入口 |
| 409 | `stream_already_exists` | 重复登记同一 `stream_id` |
| 409 | `watermark_out_of_order` | 水印早于当前记录 |
| 503 | `storage_unavailable` | 存储不可用 |
