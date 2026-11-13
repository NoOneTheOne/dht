# DHT 磁力搜索引擎

基于 BitTorrent DHT 协议的磁力链接嗅探与搜索引擎，支持 MySQL 存储、Web UI、Prometheus 监控。

## 版本控制

项目遵循语义化版本规范（Semantic Versioning），版本格式为 `vMAJOR.MINOR.PATCH`。

当前稳定版本为 **`v1.0.1`**，作为后续版本变更的基线。

### 版本号变更规则

- **Patch（`vX.Y.Z` → `vX.Y.(Z+1)`）**：小更新和修复，包括 bug 修复、安全修复、性能优化、文档调整及不改变公开行为的内部重构。
- **Minor（`vX.Y.Z` → `vX.(Y+1).0`）**：向后兼容的功能性更新，包括新增功能、API 扩展和新增可选配置项。
- **Major（`vX.Y.Z` → `v(X+1).0.0`）**：破坏性变更或大型迭代，包括不兼容的 API、配置、数据结构或协议变更，以及需要迁移的架构调整。

### 变更控制原则

1. 发布版本前必须完成代码审查、回归测试和构建验证。
2. 公开 API、配置项、数据结构或协议发生不兼容变化时，必须升级 Major 版本。
3. 功能废弃应先在 Minor 版本中标记并保留兼容周期，正式移除时升级 Major 版本。
4. 每个正式版本必须创建对应的 Git 标签，例如 `v1.0.0`，并记录本次版本的变更内容。
5. 未达到正式发布条件的版本不得占用正式版本号，可使用预发布标识，例如 `v1.1.0-rc.1`。
6. 版本号由维护者在确认变更范围后手动递增，不因普通提交自动升级。

## 功能特性

- **DHT 网络嗅探**：自动发现并抓取 BitTorrent 种子信息
- **MySQL 存储**：持久化存储种子元数据（名称、大小、infohash、热度）
- **Web 搜索界面**：炫酷的暗色主题 UI，支持排序、复制 infohash
- **RESTful API**：提供搜索和健康检查接口
- **Prometheus 监控**：完整的业务指标和系统指标暴露
- **Docker 部署**：一键 Docker Compose 启动
- **CI/CD**：GitHub Actions 自动构建部署到自托管 Runner

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
| `SEARCH_PORT` | `30080` | Search API HTTP 端口 |
| `CONFIG_FILE_PATH` | `./config.yml` | 配置文件路径（容器内） |

### 配置文件

| 字段 | 说明 |
|------|------|
| `storage.type` | 存储类型：`mysql` |
| `mysql.addresses` | MySQL 地址（host:port） |
| `mysql.username` | MySQL 用户名 |
| `mysql.password` | MySQL 密码 |
| `mysql.database` | MySQL 数据库名 |
| `mysql.table` | 数据表名（自动创建） |
| `tracker.list` | DHT Tracker 服务器列表 |

## API 文档

### 搜索接口

**请求**：
```
GET /search?name={keyword}
```

**参数**：
- `name`（必填）：搜索关键词，最大 200 字符

**响应**：
```json
{
  "total": 100,
  "items": [
    {
      "name": "示例文件名",
      "infohash": "0123456789abcdef0123456789abcdef01234567",
      "time": "2026-11-06T10:00:00Z",
      "length": 1073741824,
      "count": 5
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
- `206`：配置未加载（降级）
- `503`：MySQL 不可用（不健康）

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
- 暗色渐变动画背景 + 玻璃拟态设计
- 搜索框支持回车和按钮搜索
- 结果表格支持列排序（点击列头）
- InfoHash 完整显示 + 一键复制
- 文件大小自动转换单位（B/KB/MB/GB/TB）
- 热度徽章显示

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
go run cmd/spider/spider.go
```

4. 运行 Search API（新终端）：
```bash
go run api/search/search.go
```

### 构建 Docker 镜像

```bash
# 构建所有服务
docker compose build

# 构建单个服务
docker build --target spider -t dht-spider .
docker build --target search -t dht-search .
```

### 项目结构

```
.
├── api/
│   └── search/          # Search API 服务
├── cmd/
│   ├── spider/          # DHT Spider 嗅探器
│   └── getpeers/        # 获取 Peer 工具
├── pkg/
│   ├── basic/           # 基础工具
│   ├── config/          # 配置管理
│   ├── dht/             # DHT 协议实现
│   ├── es/              # Elasticsearch 客户端（历史遗留）
│   ├── metrics/         # Prometheus 指标
│   └── mysql/           # MySQL 客户端
├── web/                 # React 前端
├── config/
│   └── config.yml.example
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## CI/CD

项目使用 GitHub Actions 自动部署到自托管 Runner。

**工作流**：
1. Push 到 `main` 分支触发
2. 从 Runner 本地 `/volume/dht/config.yml` 复制配置
3. 构建 Docker 镜像
4. 启动服务
5. 验证服务状态
6. 清理旧镜像

**Runner 配置**：
```bash
# 在 Runner 机器上放置配置文件
sudo mkdir -p /volume/dht
sudo cp config.yml /volume/dht/config.yml
sudo chmod 600 /volume/dht/config.yml
```

## 常见问题

### Q: 启动时报 "Table doesn't exist"

A: 程序会自动创建表，如果仍然报错，请检查 MySQL 用户是否有 `CREATE TABLE` 权限。

### Q: 搜索结果为空

A: Spider 需要时间嗅探网络，刚启动时数据库可能没有数据。等待几分钟后再搜索。

### Q: 如何修改端口

A: 复制 `.env.example` 为 `.env`，修改 `DHT_PORT` 和 `SEARCH_PORT`。

### Q: 配置文件在哪里

A: 
- **本地开发**：项目根目录 `config/config.yml`
- **Docker 部署**：通过 volume 挂载 `./config/config.yml:/app/config.yml`
- **CI/CD**：从 Runner 本地 `/volume/dht/config.yml` 复制

## 协议支持

本项目实现了以下 BitTorrent DHT 协议规范：

- [BEP-3](http://www.bittorrent.org/beps/bep_0003.html) - BitTorrent Protocol（部分）
- [BEP-5](http://www.bittorrent.org/beps/bep_0005.html) - DHT Protocol
- [BEP-9](http://www.bittorrent.org/beps/bep_0009.html) - Extension for Peers to Send Metadata Files
- [BEP-10](http://www.bittorrent.org/beps/bep_0010.html) - Extension Protocol

## 致谢

本项目基于 [shiyanhui/dht](https://github.com/shiyanhui/dht) 改造，感谢原作者的贡献。

## License

MIT License