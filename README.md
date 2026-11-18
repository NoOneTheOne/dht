# DHT 磁力搜索引擎

基于 BitTorrent DHT 协议的磁力链接嗅探与搜索引擎，支持 MySQL 存储、Web UI、Prometheus 监控。

## 功能特性

- **DHT 网络嗅探**：自动发现并抓取 BitTorrent 种子信息
- **MySQL 存储**：持久化存储种子元数据（名称、大小、infohash、热度）
- **Web 搜索界面**：炫酷的暗色主题 UI，支持排序、复制 infohash
- **RESTful API**：提供搜索和健康检查接口
- **Prometheus 监控**：完整的业务指标和系统指标暴露
- **Docker 部署**：一键 Docker Compose 启动

## 架构

```
┌─────────────┐     ┌──────────────┐     ┌─────────┐
│ DHT Spider  │────▶│   MySQL DB   │◀────│  Search │
│  (嗅探器)    │     │  (种子元数据)  │     │   API   │
└─────────────┘     └──────────────┘     └────┬────┘
                                              │
                                        ┌─────┴─────┐
                                        │  Web UI   │
                                        │ (前端界面)  │
                                        └───────────┘
```

## 快速开始

### 前置条件

- Docker & Docker Compose
- MySQL 数据库（外置）
- Git（用于拉取代码）

### 1. 克隆项目

```bash
git clone https://github.com/NoOneTheOne/dht.git
cd dht
```

### 2. 配置

复制配置模板并填入你的 MySQL 连接信息：

```bash
cp config/config.yml.example config/config.yml
cp .env.example .env
vim config/config.yml
```

配置示例：

```yaml
storage:
  type: mysql

mysql:
  addresses: "your-mysql-host:3306"
  username: "your-username"
  password: "your-password"
  database: "your-database"
  table: "your-table-name"

tracker:
  list:
    - "router.bittorrent.com:6881"
    - "router.utorrent.com:6881"
    - "dht.transmissionbt.com:6881"
```

### 3. 启动服务

```bash
docker compose up -d
```

服务启动后：
- **Web UI**: http://localhost:30080
- **Search API**: http://localhost:30080/search?name=关键词
- **Health Check**: http://localhost:30080/health
- **Prometheus Metrics**: http://localhost:30080/metrics

### 4. 停止服务

```bash
docker compose down
```

## 配置说明

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `DHT_PORT` | `6881` | Spider UDP 监听端口 |
| `DHT_IMAGE_TAG` | 必填 | Docker 镜像标签，复制 `.env.example` 获取当前值 |
| `DHT_QUERY_WORKERS` | `512` | DHT 并发查询数（直接运行二进制时 `-query-workers` 默认 64） |
| `DHT_QUERY_TIMEOUT` | `5s` | 单次 DHT 查询超时 |
| `DHT_METADATA_WORKERS` | `64` | 元数据并发抓取数 |
| `DHT_PUBLIC_IP` | 空 | 其他 DHT 节点看到的公网 IP，用于生成 BEP-42 节点 ID；留空时启动时自动查询 |
| `SEARCH_PORT` | `30080` | Search API HTTP 端口 |
| `CONFIG_FILE_PATH` | `./config.yml` | 配置文件路径（容器内） |

MySQL 写入失败的批次会暂存在 `RECOVERY_FILE_PATH`（容器内默认 `/app/data/recovery.jsonl`）并每分钟重放；被 MySQL 因数据本身拒绝的单条记录会转存到同目录的 `recovery.rejected.jsonl`，不会自动重放，需人工检查。

### 配置文件

| 字段 | 说明 |
|------|------|
| `storage.type` | 存储类型：仅支持 `mysql` |
| `mysql.addresses` | MySQL 地址（host:port） |
| `mysql.username` | MySQL 用户名 |
| `mysql.password` | MySQL 密码 |
| `mysql.database` | MySQL 数据库名 |
| `mysql.table` | 数据表名（自动创建） |
| `tracker.list` | DHT 引导节点列表，格式为 `host:port` 或 `[IPv6]:port`，不是 Tracker announce URL |

`tracker.list` 会与内置 DHT 引导节点合并去重，空列表使用内置节点。`udp://…/announce`、HTTP(S) Tracker URL 不支持 DHT 引导协议，会在启动时提示并跳过，不会转换后发送 DHT 请求。

## API 文档

### 搜索接口

**请求**：
```
GET /search?name={keyword}&page={page}
```

**参数**：
- `name`（必填）：搜索关键词，最大 200 字节
- `page`（可选）：页码，从 1 开始，默认 1；每页固定 50 条

**匹配规则**：
- 关键词按空格拆分，每个词都必须出现在**资源名称或文件名**中（不分先后、可分别出现在两处），不区分大小写；名称命中的结果排在仅文件名命中的结果之前，同组内按收录时间倒序。
- 仅在文件名中命中的结果带 `"matched_files": true`。每个种子索引去重后的目录名与文件名，最多 2000 个字符，超大文件列表只索引靠前部分。
- 文件名检索单条语句限时 1.5 秒；关键词过于常见导致超时时自动改为仅匹配名称，此时响应中 `"files_searched": false`，前端会给出提示。
- 依赖 MySQL 5.7.6+ 的 ngram 全文索引，由 Search 服务启动后在后台自动创建（含存量数据回填），期间搜索自动降级为仅匹配名称；建索引会短暂阻塞写入，Spider 的写入会进入恢复文件并自动重放。数据库不支持 ngram（如 MariaDB）时回退为名称子串匹配。

**响应**：
```json
{
  "total": 100,
  "page": 1,
  "page_size": 50,
  "files_searched": true,
  "items": [
    {
      "name": "示例文件名",
      "infohash": "0123456789abcdef0123456789abcdef01234567",
      "time": "2026-11-06T10:00:00Z",
      "length": 1073741824,
      "count": 5,
      "has_files": true
    }
  ]
}
```

**字段说明**：
- `total`：匹配结果总数
- `name`：种子名称
- `infohash`：种子哈希（40 位十六进制）
- `time`：发现时间
- `length`：文件大小（字节）
- `count`：热度（被发现的次数）
- `has_files`：是否已保存文件列表（可通过详情接口获取）

### 热榜接口

```
GET /api/trending?window={1h|24h|7d}&limit={1..100}
```

按时间窗口内的采集次数排序，默认 `window=24h`、`limit=10`。返回 `items`（字段同搜索结果，`count` 为窗口内次数）及 `window`、`limit`、`since`、`until`。

### 最近收录接口

```
GET /api/recent?limit={1..100}
```

按首次收录时间倒序返回最新采集的资源，默认 `limit=10`。返回 `items`（字段同搜索结果）及 `limit`。

### 资源详情接口

```
GET /api/torrents/{infohash}
```

返回单个资源及其完整文件列表 `files`（`[{"path": [...], "length": n}]`）；未保存文件列表时返回 `404`。

### 系统状态接口

```
GET /api/status
```

汇总 Spider 状态（通过 `SPIDER_STATUS_URL` 拉取）与数据库收录统计，结果缓存 10 秒并在后台刷新，`status` 为 `operational`、`degraded` 或 `outage`，`issues` 列出具体问题。

### 健康检查

**请求**：
```
GET /health
```

**响应**：
```json
{
  "status": "ok",
  "uptime": "2h30m15s",
  "goroutines": 25,
  "memory": {
    "alloc_mb": 12,
    "total_alloc_mb": 45,
    "sys_mb": 30,
    "num_gc": 8
  },
  "config": "loaded",
  "storage_type": "mysql",
  "mysql": "connected",
  "mysql_pool": {
    "open_connections": 5,
    "in_use": 1,
    "idle": 4
  }
}
```

**状态码**：
- `200`：服务正常
- `503`：MySQL 不可用（不健康）

配置文件缺失或无法解析时，服务在启动阶段直接退出。

### Prometheus 指标

**请求**：
```
GET /metrics
```

**暴露的指标**：

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `dht_http_requests_total` | Counter | HTTP 请求总数（按方法、路径、状态码） |
| `dht_http_request_duration_seconds` | Histogram | HTTP 请求耗时分布 |
| `dht_http_request_size_bytes` | Histogram | HTTP 请求大小分布 |
| `dht_http_response_size_bytes` | Histogram | HTTP 响应大小分布 |
| `dht_search_total` | Counter | 搜索请求总数 |
| `dht_search_result_count` | Histogram | 每次搜索结果数分布 |
| `dht_search_errors_total` | Counter | 搜索错误计数（按类型） |
| `dht_mysql_query_duration_seconds` | Histogram | MySQL 查询耗时分布 |
| `dht_mysql_query_total` | Counter | MySQL 查询总数（按操作、状态） |
| `dht_mysql_connections_open` | Gauge | MySQL 打开的连接数 |
| `dht_mysql_connections_in_use` | Gauge | MySQL 使用中的连接数 |
| `dht_mysql_connections_idle` | Gauge | MySQL 空闲连接数 |
| `dht_goroutines` | Gauge | Goroutine 数量 |
| `dht_memory_alloc_bytes` | Gauge | 当前内存分配（字节） |
| `dht_memory_sys_bytes` | Gauge | 系统分配内存（字节） |
| `dht_gc_count_total` | Gauge | GC 运行总次数 |
| `dht_gc_pause_seconds` | Gauge | 最近一次 GC 暂停时间 |
| `dht_uptime_seconds` | Gauge | 服务运行时间（秒） |

## Web UI

访问 `http://localhost:30080` 打开搜索界面。

**功能**：
- 静态深色背景与响应式布局，移动端使用资源卡片
- 搜索支持回车提交、超时提示、取消旧请求及失败重试
- 结果支持键盘操作的列排序，排序仅作用于当前最多 50 条结果
- InfoHash 与磁力链接复制；点击资源名称进入详情页查看文件列表，详情页提供可唤起客户端的磁力链接
- 文件大小自动转换二进制单位（B/KiB/MiB/GiB/TiB/PiB/EiB）
- 展示成功写入元数据的采集次数，不代表在线 Peer 数

## 开发

### 本地运行

**前置条件**：
- Go 1.26+
- MySQL 数据库

**步骤**：

1. 安装依赖：
```bash
go mod download
```

2. 创建配置文件：
```bash
cp config/config.yml.example config/config.yml
# 编辑 config.yml 填入你的 MySQL 信息
```

3. 运行 Spider（嗅探器）：
```bash
CONFIG_FILE_PATH=config/config.yml go run ./cmd/spider
```

4. 运行 Search API（新终端）：
```bash
CONFIG_FILE_PATH=config/config.yml go run ./cmd/search
```

`go run` 只提供 API。Search 从可执行文件同级的 `web/` 目录加载前端，如需 Web UI：
```bash
make build-search
(cd web && npm ci && npm run build) && cp -r web/dist build/web
CONFIG_FILE_PATH=config/config.yml ./build/dht-search
```

### 构建 Docker 镜像

```bash
# 构建所有服务
docker compose build

# 构建单个服务
DHT_IMAGE_TAG=v1.0.3 docker compose build
```

### 项目结构

```
.
├── cmd/
│   ├── search/          # Search API 服务
│   ├── spider/          # DHT Spider 嗅探器
│   └── getpeers/        # 获取 Peer 工具
├── internal/
│   ├── config/          # 配置管理
│   ├── dht/             # DHT 协议实现
│   ├── metrics/         # Prometheus 指标
│   ├── nat/             # NAT 端口映射
│   └── storage/
│       └── mysql/       # MySQL 存储
├── web/                 # React 前端
├── config/
│   └── config.yml.example
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## 常见问题

### Q: 启动时报 "Table doesn't exist"

A: 程序会自动创建表，如果仍然报错，请检查 MySQL 用户是否有 `CREATE TABLE` 权限。

### Q: 搜索结果为空

A: Spider 需要时间嗅探网络，刚启动时数据库可能没有数据。等待几分钟后再搜索。

### Q: `_observations` 表是做什么的？会一直增长吗？

A: 每批写入都会在 `<table>_observations` 中记录观测明细，用于批次幂等（故障恢复重放不会重复计数）和热榜统计。Spider 每小时删除 8 天前已应用的记录（热榜最长窗口为 7 天），表大小保持稳定。

### Q: 如何修改端口

A: 复制 `.env.example` 为 `.env`，修改 `DHT_PORT` 和 `SEARCH_PORT`。

### Q: 配置文件在哪里

A: 
- **本地开发**：项目根目录 `config/config.yml`
- **Docker 部署**：通过 volume 挂载 `./config/config.yml:/app/config.yml`

## 协议支持

本项目实现了以下 BitTorrent 协议规范：

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