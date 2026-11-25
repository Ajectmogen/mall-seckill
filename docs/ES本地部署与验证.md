# Elasticsearch 接入说明与本地验证记录

`mall-search` 是原版 mall 自带的搜索模块，负责商品搜索、筛选、推荐和相关词。这一块平时很难验证，因为要单独装 ES 和 IK 分词器。我本地起了一次，把导入、搜索、推荐的接口都跑了一遍，记录如下。

## 1. 模块与配置

| 项 | 值 | 位置 |
|---|---|---|
| 模块 | `mall-search` | `mall-search/pom.xml` |
| 端口 | **8186** | `mall-search/src/main/resources/application.yml:11`（注释：`mall-1 副本，偏移 +105`） |
| ES 地址 | `localhost:9200` | `mall-search/src/main/resources/application-dev.yml:20` |
| 索引名 | `pms` | `mall-search/src/main/java/com/macro/mall/search/domain/EsProduct.java:21`（`@Document(indexName = "pms")`） |
| 版本 | Elasticsearch **8.18.8** + analysis-ik **8.18.8** | 本地运行时（未入库） |

> 端口为什么是 8186 而不是 8185+1：原版 `mall-search/application.yml` 写的是 **8081**，和 `mall-admin` 撞车，本来就起不来。所以本副本给 search 单独偏移 **+105**，其余模块统一 +100（admin 8181、portal 8185）。

## 2. 接口清单

统一前缀 `/esProduct`（`mall-search/src/main/java/com/macro/mall/search/controller/EsProductController.java:26`）：

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/esProduct/importAll` | 全量导入数据库商品到 ES |
| POST | `/esProduct/create/{id}` | 单个商品建索引 |
| GET | `/esProduct/delete/{id}` | 删除单个文档 |
| POST | `/esProduct/delete/batch` | 批量删除 |
| GET | `/esProduct/search/simple?keyword=` | 简单搜索（只有关键词） |
| GET | `/esProduct/search` | 完整搜索（关键词 + 品牌/分类/分类筛选 + 排序 + 分页） |
| GET | `/esProduct/recommend/{id}` | 相关商品推荐 |
| GET | `/esProduct/search/relate` | 搜索相关词（品牌/分类/属性聚合） |

## 3. 本地跑通步骤

```bash
# 1) 起 ES（8.18.8，IK 分词器放进 plugins 目录）
#    单节点、关闭安全认证的本地实验配置
# 2) 起 mall-search（先起 MySQL/Redis，登录 token 需要 mall-admin 或 portal 的认证体系）
java -jar mall-search/target/mall-search-1.0-SNAPSHOT.jar --spring.profiles.active=dev

# 3) 全量导入
curl -X POST 'http://localhost:8186/esProduct/importAll'

# 4) 验证中文搜索
curl 'http://localhost:8186/esProduct/search/simple?keyword=手机'
```

## 4. 实测结果

| 验证项 | 结果 |
|---|---|
| `POST /esProduct/importAll` | 返回 `data: 20` —— 库里有 **20 个商品**被写入 `pms` 索引 |
| `GET /esProduct/search/simple?keyword=手机` | 返回 `total: 5` —— 20 个商品里 5 个命中"手机" |
| 分词 | IK 生效，中文按词切分（不是单字切） |

> 一个容易看错的地方：`EsProductDao.xml` 的联表 SQL 直接查会返回 **92 行**，但那是"商品 × 属性值"展开后的行数（一个商品有多个属性值），**不是商品数**。真实商品数是 `importAll` 返回的 20。

## 5. 踩过的坑

1. **内存**：ES + mall-portal + mall-search 同时跑，把本机内存撑爆，`mall-portal` 直接崩了，`hs_err_pid*.log` 里是
   `Native memory allocation (mmap) failed to map 266338304 bytes for G1 virtual space`。
   排查顺序是：先 `-Xmx` 收敛各 JVM，或者验证 ES 时先把 portal 停掉。
2. **端口撞车**：见上文，原版 search 的 8081 与 admin 相同。
3. **索引要先建**：ES 8 默认不会自动建 mapping，`importAll` 之前确认 `pms` 索引存在，否则字段类型会按首次写入推断，后面改要重建。

## 6. 还没做的

- 没有把 ES 做成开箱即用的部署脚本（现在是手工解压 + 手工起进程），`document/docker/docker-compose*.yml` 里有中间件编排，但没有纳入 ES。
- 没有做增量同步：商品增删改之后要靠手动调 `/esProduct/create/{id}`、`/esProduct/delete/{id}`，没有监听 binlog / 发 MQ 的自动同步链路。
- 没有压测搜索链路，只有功能验证。