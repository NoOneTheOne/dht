# DHT 磁力搜索引擎

基于 BitTorrent DHT 协议的磁力元数据采集与搜索引擎：Spider 从 DHT 网络发现 infohash 并向 Peer 抓取种子元数据，存入 MySQL；Search 服务提供按名称与文件名检索的 API 和 Web 界面。

## 功能特性

- **DHT 采集**：BEP-5 爬虫模式，结合入站 `get_peers`/`announce_peer`、主动迭代查找与 BEP-51 `sample_infohashes` 采样发现资源。
- **元数据抓取**：BEP-9/BEP-10 下载种子信息并校验 SHA1，优先 MSE 加密握手（兼容强制加密的客户端），对方拒绝时回退明文。
- **全文搜索**：按名称与文件名检索（MySQL ngram 全文索引，中英文均可），名称命中优先；常见词超时自动降级为仅匹配名称。
- **实时热榜 / 最近收录 / 资源详情**：按 1 小时、24 小时、7 天统计采集热度；实时展示最新收录；详情页列出完整文件列表。
- **系统状态**：展示采集进程就绪、公网可达性、NAT、并发与成功率、收录统计。
- **可靠写入**：批次幂等（故障重放不重复计数）、MySQL 不可用时落盘恢复文件并自动重放、数据被拒的单条记录单独隔离。
- **Prometheus 指标与结构化 JSON 日志**；**Docker Compose 部署**，GitHub Actions 按版本标签自动发布与部署。

## 架构

```
            UDP 6881                                  HTTP 30080
 DHT 网络 ◀────────▶ ┌─────────────┐   写入    ┌───────┐   查询   ┌──────────────┐ ◀── 浏览器 / API
 BitTorrent Peer ◀── │ dht-spider  │ ───────▶ │ MySQL │ ◀────── │  dht-search  │
      (TCP 元数据)    │   (采集)     │          └───────┘          │ (API + Web)  │
                     └──────┬──────┘                              └──────┬───────┘
                            └──── /status、/metrics（30081，仅本机）──────┘
```

## 快速开始

### 前置条件

- Docker 与 Docker Compose
- MySQL 5.7.6+ 或 8.x（外置；需支持 ngram 全文解析器，MariaDB 不支持，见[常见问题](#常见问题)）
- 公网 UDP 端口（默认 6881）转发到运行 Spider 的主机

### 1. 克隆项目

```bash
git clone https://github.com/NoOneTheOne/dht.git
cd dht
```

### 2. 配置

所有配置都通过环境变量提供，Docker Compose 会自动读取项目根目录的 `.env`：

```bash
cp .env.example .env
vim .env   # 至少填写 DHT_MYSQL_* 五项
```

最小配置示例：

```dotenv
DHT_IMAGE_TAG=v1.12.1
DHT_MYSQL_ADDRESS=10.0.0.10:3306
DHT_MYSQL_USER=dht
DHT_MYSQL_PASSWORD=change-me
DHT_MYSQL_DATABASE=DHT
DHT_MYSQL_TABLE=info
```

数据表、观测表和全文索引都会自动创建，MySQL 用户需要对该库有 `CREATE`、`ALTER`、`INDEX`、`SELECT`、`INSERT`、`UPDATE`、`DELETE` 权限。

### 3. 启动

```bash
docker compose up -d
```

启动后访问：

| 地址 | 说明 |
|------|------|
| http://localhost:30080 | Web 界面 |
| http://localhost:30080/search?name=关键词 | 搜索 API |
| http://localhost:30080/health | 健康检查 |
| http://localhost:30080/metrics | Search 的 Prometheus 指标 |
| http://127.0.0.1:30081/metrics | Spider 的 Prometheus 指标（仅本机可访问） |

刚启动时数据库为空，Spider 加入 DHT 网络后通常几分钟内开始收录。

### 4. 停止

```bash
docker compose down
```

Spider 停止时会先把待写入的数据落库（宽限期 60 秒），不会丢数据。

## 配置说明

### 必填：MySQL

缺少任何一项时，`docker compose config` / `up` 会直接报错并指出变量名；直接运行二进制时，程序启动即退出并列出缺少的变量。

| 变量 | 说明 |
|------|------|
| `DHT_MYSQL_ADDRESS` | MySQL 地址，`host:port` |
| `DHT_MYSQL_USER` | 用户名 |
| `DHT_MYSQL_PASSWORD` | 密码（可为空；原样使用，可包含空格） |
| `DHT_MYSQL_DATABASE` | 数据库名 |
| `DHT_MYSQL_TABLE` | 资源表名，自动创建；观测表为 `<表名>_observations` |

### 采集（Spider）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DHT_PORT` | `6881` | DHT UDP 监听端口；需把相同的公网 UDP 端口转发到本机 |
| `DHT_QUERY_WORKERS` | `512` | 并发 DHT 查询数 |
| `DHT_QUERY_TIMEOUT` | `5s` | 单次 DHT 查询超时 |
| `DHT_METADATA_WORKERS` | `64` | 并发元数据下载（TCP 连接）数 |
| `DHT_PUBLIC_IP` | 空 | 其他节点看到的公网 IP，用于生成 BEP-42 节点 ID；留空则启动时自动查询。动态 IP 建议留空 |
| `DHT_BOOTSTRAP_NODES` | 空 | 额外的 DHT 引导节点，`host:port` 或 `[IPv6]:port`，逗号分隔；内置节点始终使用 |

`DHT_BOOTSTRAP_NODES` 只接受 DHT 节点地址。`udp://…/announce`、`http(s)://…` 这类 Tracker 地址不是 DHT 节点，启动时会提示并跳过。

### 服务与部署

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DHT_IMAGE_TAG` | 必填 | Compose 使用的镜像版本，取值见 `.env.example`（CI 部署时由 Git 标签决定） |
| `SEARCH_PORT` | `30080` | 宿主机上 Search 的 HTTP 端口 |
| `SPIDER_STATUS_PORT` | `30081` | 宿主机上 Spider 状态与指标端口，仅绑定 `127.0.0.1` |

### 高级（一般无需修改）

以下变量在 `docker-compose.yml` 或镜像中已设置合适的值，直接运行二进制时可按需设置：

| 变量 | 使用方 | 默认值 | 说明 |
|------|--------|--------|------|
| `RECOVERY_FILE_PATH` | Spider | 容器内 `/app/data/recovery.jsonl`，否则系统临时目录 | MySQL 写入失败时的恢复文件；被拒数据写入同目录的 `*.rejected.jsonl` |
| `NODE_CACHE_PATH` | Spider | 恢复文件同目录的 `dht-nodes.json` | 已验证 DHT 节点缓存，重启后加速加入网络 |
| `SPIDER_STATUS_ADDR` | Spider | `127.0.0.1:30081`（Compose 中为 `:30081`） | 状态与指标接口（`/status`、`/metrics`）监听地址 |
| `SPIDER_STATUS_URL` | Search | `http://127.0.0.1:30081/status` | Search 读取采集状态的地址 |
| `SEARCH_API_PORT` | Search | `30080` | 容器内 HTTP 监听端口 |
| `LOG_FILE_PATH` | 全部 | 空（输出到标准输出） | 设置后日志改写入该文件 |
| `APP_ENV` | 全部 | 构建时注入 | 写入每条日志的环境标识 |

Spider 命令行参数（Compose 已通过上面的变量传入）：`-port`、`-query-workers`（直接运行时默认 64）、`-query-timeout`、`-metadata-workers`、`-nat`（UPnP/NAT-PMP 自动端口映射，默认关闭；手动端口转发或 Docker bridge 网络下保持关闭）。

## 生产部署（CI/CD）

推送 `vX.Y.Z` 标签会触发 `.github/workflows/deploy.yml`：测试、构建、打包 5 个平台的发布包并发布 GitHub Release，最后在自托管 Runner 上部署。

部署机准备：

1. 在 Runner 主机创建 **`/volume/dht/.env`**（必需，权限建议 `600`），写入 `DHT_MYSQL_*` 等配置；无需写 `DHT_IMAGE_TAG`，部署时以标签为准。
2. 发版：把 `.env.example` 中的 `DHT_IMAGE_TAG` 改为新版本号，提交到 `main` 后推送同名标签。

部署流程会校验标签与 `.env.example` 一致、提交位于 `main` 上、不会用旧版本覆盖新版本，缺少必填配置时在启动容器前失败。

### 从 `config.yml` 迁移

v1.12.1 起不再读取 `config.yml`（包括部署机上的 `/volume/dht/config.yml`）。按下表把原配置写入 `.env`（部署机为 `/volume/dht/.env`）：

| 原 `config.yml` 字段 | 新环境变量 |
|------|------|
| `mysql.addresses` | `DHT_MYSQL_ADDRESS` |
| `mysql.username` | `DHT_MYSQL_USER` |
| `mysql.password` | `DHT_MYSQL_PASSWORD` |
| `mysql.database` | `DHT_MYSQL_DATABASE` |
| `mysql.table` | `DHT_MYSQL_TABLE` |
| `tracker.list` | `DHT_BOOTSTRAP_NODES`（逗号分隔） |
| `storage.type` | 删除（仅支持 MySQL） |
| `elasticsearch.*` | 删除（v1.12.0 起不再支持 Elasticsearch） |

迁移完成后可删除 `config.yml`。

## API 文档

所有接口均为 `GET`，返回 JSON；错误响应为 `{"error": "..."}`，不包含内部细节。

### 搜索

```
GET /search?name={关键词}&page={页码}
```

- `name`（必填）：最长 200 字节
- `page`（可选）：从 1 开始，默认 1，最大 200；每页固定 50 条

**匹配规则**：

- 关键词按空格拆分，每个词都必须出现在**资源名称或文件名**中（顺序不限，可分别出现在两处），不区分大小写。
- 名称命中的结果排在仅文件名命中的结果之前；同组内按收录时间倒序。仅文件名命中的结果带 `"matched_files": true`。
- 每个种子索引去重后的目录名与文件名，最多 2000 个字符，超大文件列表只索引靠前部分。
- 文件名检索单条语句限时 1.5 秒；关键词过于常见导致超时时自动改为仅匹配名称，此时 `"files_searched": false`。
- 仅匹配名称的检索单条语句限时 3 秒；仍超时说明关键词过于宽泛，返回 `422`（`{"error": "search too broad"}`），请补充更具体的关键词。
- 匹配总数最多统计到 10000 条，超过时 `total` 为 10000 且 `"total_capped": true`（页面显示「10,000+」），可翻页至第 200 页。
- 全文索引由 Search 启动后在后台自动创建或升级（约十几秒，期间搜索降级为仅匹配名称，Spider 的写入短暂排队）。数据库不支持 ngram 时回退为名称子串匹配。

**响应**：

```json
{
  "total": 100,
  "page": 1,
  "page_size": 50,
  "files_searched": true,
  "total_capped": false,
  "items": [
    {
      "name": "示例资源",
      "infohash": "0123456789abcdef0123456789abcdef01234567",
      "time": "2026-11-06T10:00:00Z",
      "length": 1073741824,
      "count": 5,
      "has_files": true,
      "matched_files": true
    }
  ]
}
```

| 字段 | 说明 |
|------|------|
| `total` | 匹配总数，最多 10000 |
| `total_capped` | 为 `true` 时实际匹配超过 10000 条 |
| `files_searched` | 本次是否检索了文件名 |
| `name` / `infohash` | 种子名称 / 40 位十六进制 infohash |
| `time` | 首次收录时间（UTC） |
| `length` | 总大小（字节，已排除 BEP-47 填充文件） |
| `count` | 成功采集并落库的次数（不是在线 Peer 数或下载量） |
| `has_files` | 是否有文件列表（可通过详情接口获取） |
| `matched_files` | 仅在文件名中命中时出现，值为 `true` |

### 热榜

```
GET /api/trending?window={1h|24h|7d}&limit={1..100}
```

按时间窗口内的采集次数排序，默认 `window=24h`、`limit=10`。返回 `items`（字段同搜索，`count` 为窗口内次数）及 `window`、`limit`、`since`、`until`。排行由 Search 在后台计算前 100 名并缓存（1h 窗口 1 分钟、24h 窗口 5 分钟、7d 窗口 15 分钟），`until` 为本次排行的计算时间；启动后首次计算完成前，请求会等待结果。热榜查询使用覆盖索引 `idx_observation_rank`，由 Search 启动后在后台在线创建。

### 最近收录

```
GET /api/recent?limit={1..100}
```

按首次收录时间倒序返回最新资源，默认 `limit=10`。返回 `items` 与 `limit`。

### 资源详情

```
GET /api/torrents/{infohash}
```

返回资源信息及完整文件列表 `files`（`[{"path": ["目录", "文件名"], "length": 字节}]`）；无文件列表时返回 `404`。

### 系统状态

```
GET /api/status
```

汇总采集进程状态（经 `SPIDER_STATUS_URL` 获取）与收录统计，缓存 10 秒。数据库是否可用以连接探测（ping）为准；收录统计需扫描全表，单独在后台计算并缓存 10 分钟（失败 1 分钟后重试，期间保留上次结果），统计失败只提示「入库统计暂时不可用」。缓存刚过期时先返回旧值并在后台刷新；过期已久（如空闲后首次访问）时最多等待 5 秒取得最新状态。`status` 为 `operational`、`degraded` 或 `outage`，`issues` 列出具体问题。

### 健康检查

```
GET /health
```

```json
{
  "status": "ok",
  "uptime": "2h30m15s",
  "goroutines": 25,
  "memory": { "alloc_mb": 12, "total_alloc_mb": 45, "sys_mb": 30, "num_gc": 8 },
  "config": "loaded",
  "storage_type": "mysql",
  "mysql": "connected",
  "mysql_pool": { "open_connections": 5, "in_use": 1, "idle": 4 }
}
```

MySQL 可用时返回 `200`，不可用时返回 `503`。配置不完整时服务在启动阶段直接退出，不会进入运行状态。

## Web 界面

访问 `http://localhost:30080`，右上角导航包含四个页面，支持日间 / 夜间主题切换（自动记住选择）：

- **搜索服务**：按名称与文件名搜索，每页 50 条，支持翻页与当前页内的列排序；仅文件名命中的结果带"文件名命中"标签，降级为仅匹配名称时给出提示。
- **实时热榜**：按 1 小时 / 24 小时 / 7 天排行，条数 1–100 可调。
- **最近收录**：按收录时间倒序，条数 1–100 可调，页面打开时每 30 秒自动刷新。
- **系统状态**：采集进程就绪与可达性、NAT、并发与成功率、收录与文件列表覆盖率。

点击资源名称进入详情页查看文件列表；InfoHash 与磁力链接可一键复制。界面适配手机。

## 监控与日志

两个服务各自提供 Prometheus 指标，抓取时需要同时配置：

| 服务 | 地址 | 内容 |
|------|------|------|
| Search | `http://<主机>:30080/metrics` | HTTP、搜索、MySQL 查询与连接池、进程运行状态 |
| Spider | `http://127.0.0.1:30081/metrics` | DHT 网络、协议各环节结果、队列与并发、元数据下载耗时、进程运行状态 |

### Search 指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `dht_http_requests_total` | Counter | HTTP 请求数（方法、路由、状态码） |
| `dht_http_request_duration_seconds` | Histogram | HTTP 请求耗时 |
| `dht_http_request_size_bytes` / `dht_http_response_size_bytes` | Histogram | 请求 / 响应大小 |
| `dht_search_total` | Counter | 搜索次数 |
| `dht_search_result_count` | Histogram | 每次搜索结果数 |
| `dht_search_errors_total` | Counter | 搜索错误（按类型） |
| `dht_mysql_query_duration_seconds` | Histogram | MySQL 查询耗时 |
| `dht_mysql_query_total` | Counter | MySQL 查询数（操作、结果） |
| `dht_mysql_connections_open` / `_in_use` / `_idle` | Gauge | 连接池状态 |
| `dht_goroutines`、`dht_memory_alloc_bytes`、`dht_memory_sys_bytes`、`dht_gc_count_total`、`dht_gc_pause_seconds`、`dht_uptime_seconds` | Gauge | 进程运行状态 |

### Spider 指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `dht_routing_table_nodes` | Gauge | 路由表节点数 |
| `dht_active_transactions` / `dht_blacklisted_nodes` | Gauge | 进行中的 DHT 查询 / 黑名单节点数 |
| `dht_bootstrap_total` | Counter | 引导（加入网络）次数 |
| `dht_packets_received_total` / `dht_packets_sent_total` / `dht_packets_dropped_total` | Counter | UDP 包收发与丢弃 |
| `dht_torrents_discovered_total` | Counter | 发现的 infohash 次数 |
| `dht_metadata_requests_dropped_total` / `dht_metadata_responses_dropped_total` | Counter | 因队列满等原因丢弃的元数据请求 / 结果 |
| `dht_protocol_events_total` | Counter | 各协议环节结果（`stage`、`reason`），如元数据下载成败、加密握手、BEP-51 采样 |
| `dht_queue_depth` / `dht_active_workers` | Gauge | 各队列积压与各工作池并发（`queue` / `pool`） |
| `dht_metadata_stage_duration_seconds` | Histogram | 元数据下载各阶段耗时（`stage`、`result`） |

示例：元数据下载成功率可用 `sum(rate(dht_protocol_events_total{stage="metadata",reason="success"}[5m])) / sum(rate(dht_protocol_events_total{stage="metadata",reason!="stopped"}[5m]))`。

### 采集状态

- Web 界面的"系统状态"页，或 `GET /api/status`；Spider 原始状态见 `http://127.0.0.1:30081/status`。
- Spider 每分钟输出一条 `dht_protocol_summary` 日志，包含各协议环节的计数与队列深度：`docker logs dht-spider | grep dht_protocol_summary | tail -1`。

### 日志

两个服务都输出 JSON 结构化日志（标准输出，或 `LOG_FILE_PATH` 指定的文件），每条带 `service`、`version`、`revision`、`environment`；HTTP 请求日志带 `request_id`，响应头 `X-Request-ID` 与之对应。

## 开发

### 本地运行

前置条件：Go 1.26+、Node.js 20+（仅构建前端时需要）、MySQL。

```bash
go mod download
cp .env.example .env && vim .env      # 填写 DHT_MYSQL_*
set -a && . ./.env && set +a          # 导出到当前 shell

go run ./cmd/spider                   # 采集（另开终端运行 search）
go run ./cmd/search                   # API，默认 :30080
```

`go run` 只提供 API。Search 从可执行文件同级的 `web/` 目录加载前端，需要 Web 界面时：

```bash
make build-search
(cd web && npm ci && npm run build) && cp -r web/dist build/web
./build/dht-search
```

单独查询某个 infohash 的 Peer（不需要 MySQL）：

```bash
go run ./cmd/getpeers -infohash 0123456789abcdef0123456789abcdef01234567
```

### 测试

```bash
make test        # go test -race ./...
make vet
```

全文搜索在真实 MySQL 上的集成测试默认跳过，需要时：

```bash
docker run --rm -d --name dht-mysql-test -e MYSQL_ALLOW_EMPTY_PASSWORD=yes \
  -e MYSQL_DATABASE=ftscheck -p 127.0.0.1:33306:3306 mysql:8.4
DHT_TEST_MYSQL_ADDR=127.0.0.1:33306 go test -run Fulltext ./internal/storage/mysql/
docker rm -f dht-mysql-test
```

### 构建镜像

```bash
docker compose build     # 镜像标签取自 .env 中的 DHT_IMAGE_TAG
```

### 项目结构

```
.
├── cmd/
│   ├── spider/          # 采集进程：DHT、调度、元数据下载、批量写入与恢复
│   ├── search/          # API 与 Web 静态资源服务
│   └── getpeers/        # 命令行工具：查询 infohash 的 Peer
├── internal/
│   ├── config/          # 环境变量配置
│   ├── dht/             # DHT（BEP-5/42/51）、Peer Wire（BEP-3/9/10）、MSE
│   ├── logging/         # 结构化日志
│   ├── metrics/         # Prometheus 指标与状态模型
│   ├── nat/             # UPnP / NAT-PMP 端口映射
│   └── storage/mysql/   # 存储、全文搜索、恢复文件
├── web/                 # React + Vite 前端
├── .env.example         # 配置模板
├── docker-compose.yml
└── Dockerfile
```

## 常见问题

### 启动时报 `missing required environment variables`

`.env` 中缺少列出的变量，参照 `.env.example` 补全。部署机检查 `/volume/dht/.env`。

### 搜索结果为空

刚启动时数据库为空，Spider 加入 DHT 网络后才会开始收录，通常需要几分钟。可在"系统状态"页确认采集进程已就绪、公网 UDP 可达。

### 新收录的资源搜不到

确认 Search 日志中出现过 `search_indexes_ready`；全文索引建好前搜索只按名称匹配。v1.12.0 存在一个写入缺陷，会导致部分新资源未进入全文索引；升级到 v1.12.1 后，Search 首次启动会自动重建索引并找回这些资源（日志中可见 `*_outdated_dropped`）。

### 数据库是 MariaDB

MariaDB 没有 ngram 全文解析器，无法创建全文索引。服务仍可运行，搜索回退为名称子串匹配（不检索文件名，数据量大时较慢），Search 日志会记录建索引失败。

### `_observations` 表是做什么的？会一直增长吗？

它记录每批写入的明细，用于批次幂等（故障重放不重复计数）和热榜统计。写入完成后会清空其中的文件列表，Spider 每小时删除 8 天前的记录（热榜最长窗口为 7 天），表大小保持稳定。

### 如何修改端口

修改 `.env` 中的 `DHT_PORT`（UDP，需同步调整公网端口转发）和 `SEARCH_PORT`，然后 `docker compose up -d`。

## 协议支持

- [BEP-3](https://www.bittorrent.org/beps/bep_0003.html) - BitTorrent Protocol（握手与消息帧，仅用于抓取元数据）
- [BEP-5](https://www.bittorrent.org/beps/bep_0005.html) - DHT Protocol（含未知方法的 204 错误响应）
- [BEP-9](https://www.bittorrent.org/beps/bep_0009.html) - Extension for Peers to Send Metadata Files
- [BEP-10](https://www.bittorrent.org/beps/bep_0010.html) - Extension Protocol
- [BEP-20](https://www.bittorrent.org/beps/bep_0020.html) - Peer ID Conventions
- [BEP-42](https://www.bittorrent.org/beps/bep_0042.html) - DHT Security Extension（按公网 IP 生成节点 ID，响应中携带 `ip` 字段）
- [BEP-47](https://www.bittorrent.org/beps/bep_0047.html) - Padding files（文件列表中忽略填充文件）
- [BEP-51](https://www.bittorrent.org/beps/bep_0051.html) - DHT Infohash Indexing（`sample_infohashes` 采样，遵守对方的 `interval`）
- [Message Stream Encryption](https://wiki.vuze.com/w/Message_Stream_Encryption)（MSE/PE）- 元数据连接优先加密握手，对方拒绝时回退明文

## 致谢

本项目基于 [shiyanhui/dht](https://github.com/shiyanhui/dht) 改造，感谢原作者的贡献。

## License

MIT License
