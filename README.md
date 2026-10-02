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

登记数据流。请求体：

```json
{"stream_id":"orders","window_type":"tumbling","window_size_seconds":60,"allowed_lateness_seconds":10}
```

- `stream_id`、`window_type`：非空字符串
- `window_size_seconds`：大于零的数值
- `allowed_lateness_seconds`：非负数值

成功返回 HTTP 201，并回显服务保存的四项字段。重复登记同一 `stream_id` 返回 HTTP 409（`stream_already_exists`），原记录保持不变。

### `POST /api/v1/streams/{stream_id}/watermarks`

提交水印进度。请求体：

```json
{"watermark":1696000000}
```

`watermark` 为非负整数 Unix 秒。高于当前水印时推进记录，等于当前水印时成功且保持不变，两种情况都返回 HTTP 200 及当前记录：

```json
{"stream_id":"orders","watermark":1696000000}
```

早于当前水印的提交返回 HTTP 409（`watermark_out_of_order`）。比较与写入在同一原子操作中完成，并发提交的最终结果是参与者中的最高水印，不会被较晚到达的较低值覆盖。

### `GET /api/v1/streams/{stream_id}/watermark`

返回当前水印记录，字段与提交响应相同；新登记数据流的初始水印为 `0`。未登记的 `stream_id` 返回 HTTP 404（`stream_not_found`）。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。

| code | HTTP | 含义 |
|---|---|---|
| `invalid_request` | 400 | 字段缺失、类型不符、窗口长度非正或水印非法 |
| `stream_not_found` | 404 | 路径中的 `stream_id` 未登记 |
| `stream_already_exists` | 409 | 重复登记同一 `stream_id` |
| `watermark_out_of_order` | 409 | 提交的水印早于当前水印 |
| `route_not_found` | 404 | 没有匹配的路由 |
| `storage_unavailable` | 503 | 存储不可用 |
