# Flink Checkpoint 从 SeaweedFS 迁移到 MinIO — 技术细节与测试文档

> 本文档是 [README.md](README.md) 的补充，深入讲解技术实现细节、架构设计、测试策略和迁移原理。

---

## 目录

1. [技术架构详解](#1-技术架构详解)
2. [核心代码深度解析](#2-核心代码深度解析)
3. [Flink Checkpoint 机制原理](#3-flink-checkpoint-机制原理)
4. [SeaweedFS vs MinIO 深度对比](#4-seaweedfs-vs-minio-深度对比)
5. [测试策略与设计](#5-测试策略与设计)
6. [S3 协议适配细节](#6-s3-协议适配细节)
7. [生产环境部署建议](#7-生产环境部署建议)
8. [性能优化指南](#8-性能优化指南)

---

## 1. 技术架构详解

### 1.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        Spring Boot 应用                          │
│                                                                  │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐    │
│  │ JobController │──▶│CheckpointSvc │──▶│  MinioInitSvc    │    │
│  │  (REST API)  │   │ (作业管理)    │   │  (Bucket 初始化) │    │
│  └──────────────┘   └──────┬───────┘   └────────┬─────────┘    │
│                             │                     │              │
│  ┌──────────────┐   ┌──────▼───────┐   ┌────────▼─────────┐    │
│  │ WordCountJob │   │  FlinkConfig │   │ MinioProperties  │    │
│  │ (Flink 作业) │   │ (环境配置)   │   │ (连接配置)       │    │
│  └──────────────┘   └──────────────┘   └──────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ S3 协议
                              ▼
                    ┌──────────────────┐
                    │      MinIO       │
                    │  (S3 兼容存储)   │
                    │                  │
                    │ Bucket:          │
                    │ flink-checkpoints│
                    │   └─ flink/      │
                    │      checkpoints/│
                    │        ├─ chk-1/ │
                    │        ├─ chk-2/ │
                    │        └─ ...    │
                    └──────────────────┘
```

### 1.2 组件职责

| 组件 | 职责 | 关键注解/类 |
|------|------|------------|
| `FlinkMinioApplication` | Spring Boot 入口 | `@SpringBootApplication` |
| `FlinkConfig` | 配置 Flink 执行环境、Checkpoint、状态后端 | `@Configuration`, `@Bean` |
| `MinioProperties` | 从 yml 读取 MinIO 连接信息 | `@ConfigurationProperties(prefix="minio")` |
| `MinioClientConfig` | 创建 MinIO Java SDK 客户端 | `@Configuration`, `@Bean` |
| `WordCountJob` | Flink 流处理作业逻辑 | 静态方法 |
| `CheckpointService` | 作业提交和 Checkpoint 查询 | `@Service` |
| `MinioInitService` | MinIO Bucket 初始化 | `@Service` |
| `JobController` | REST API 接口 | `@RestController` |

### 1.3 数据流

```
用户请求 → JobController → CheckpointService → FlinkConfig (创建执行环境)
                                              → WordCountJob (执行作业)
                                              → MinIO (写入 Checkpoint)
                                              → MinioInitService (确保 Bucket 存在)
```

---

## 2. 核心代码深度解析

### 2.1 FlinkConfig — 迁移的核心

这是整个项目最关键的类，包含了从 SeaweedFS 迁移到 MinIO 的所有配置修改点。

#### 状态后端选择

```java
// RocksDB 状态后端：适合大状态场景
// 参数 true 表示启用增量 Checkpoint
EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
env.setStateBackend(rocksDBStateBackend);
```

**为什么选择 RocksDB？**

| 特性 | HashMapStateBackend | EmbeddedRocksDBStateBackend |
|------|---------------------|---------------------------|
| 状态存储位置 | JVM 堆内存 | 本地 RocksDB（磁盘） |
| 状态大小限制 | 受 JVM 堆内存限制 | 仅受磁盘大小限制 |
| 增量 Checkpoint | 不支持 | 支持 |
| GC 影响 | 大状态时 GC 压力大 | 几乎无 GC 压力 |
| 适用场景 | 小状态（<100MB） | 大状态（>100MB） |

增量 Checkpoint 的好处：只保存变化的状态数据，减少写入 MinIO 的数据量，提高 Checkpoint 速度。

#### S3 配置详解

```java
Configuration flinkConfig = new Configuration();

// 【关键配置1】S3 endpoint
// 告诉 Flink 的 S3 插件连接 MinIO 而不是 AWS
// 这是迁移的核心：SeaweedFS 不需要此配置
flinkConfig.setString("s3.endpoint", "http://localhost:9000");

// 【关键配置2】访问密钥
// MinIO 的 access key 和 secret key
flinkConfig.setString("s3.access-key", "minioadmin");
flinkConfig.setString("s3.secret-key", "minioadmin");

// 【关键配置3】路径风格访问
// MinIO 使用路径风格：http://minio:9000/bucket/object
// AWS 使用虚拟主机风格：http://bucket.s3.amazonaws.com/object
// 必须设为 true，否则 MinIO 无法正确路由请求
flinkConfig.setString("s3.path.style.access", "true");
```

#### Checkpoint 存储路径

```java
// 【迁移核心修改点】
// SeaweedFS: "file:///mnt/seaweedfs/flink/checkpoints"
// MinIO:     "s3://flink-checkpoints/flink/checkpoints"
checkpointConfig.setCheckpointStorage("s3://flink-checkpoints/flink/checkpoints");
```

### 2.2 WordCountJob — 流处理作业

本作业演示了 Flink 流处理的基本模式：

```
数据源(Source) → 转换(Transform) → 窗口(Window) → 聚合(Aggregate) → 输出(Sink)
```

#### 关键算子说明

| 算子 | 类型 | 作用 |
|------|------|------|
| `fromElements` | Source | 从内存集合创建数据流 |
| `flatMap(Tokenizer)` | Transform | 将一行文本拆分为 (单词, 1) 元组 |
| `keyBy` | Partition | 按单词分组（相同单词到同一并行实例） |
| `window` | Window | 每 5 秒一个滚动窗口 |
| `reduce` | Aggregate | 在窗口内累加计数 |
| `print` | Sink | 输出到控制台 |

#### 窗口机制

```
时间轴: |---5s---|---5s---|---5s---|
窗口1:  [数据1, 数据2, ...]
窗口2:  [数据3, 数据4, ...]
窗口3:  [数据5, ...]

每个窗口结束时触发一次计算，输出该窗口内的统计结果
```

### 2.3 MinioProperties — 配置映射

```java
@ConfigurationProperties(prefix = "minio")
public class MinioProperties {
    private String endpoint = "http://localhost:9000";
    private String accessKey = "minioadmin";
    private String secretKey = "minioadmin";
    private String bucket = "flink-checkpoints";
    private String checkpointPath = "flink/checkpoints";

    // 获取完整的 S3 路径
    public String getFullCheckpointPath() {
        return "s3://" + bucket + "/" + checkpointPath;
    }
}
```

**配置映射关系**：

```yaml
# application.yml
minio:                          # @ConfigurationProperties(prefix="minio")
  endpoint: http://localhost:9000  → minioProperties.getEndpoint()
  access-key: minioadmin           → minioProperties.getAccessKey()
  secret-key: minioadmin           → minioProperties.getSecretKey()
  bucket: flink-checkpoints        → minioProperties.getBucket()
  checkpoint-path: flink/checkpoints → minioProperties.getCheckpointPath()
```

---

## 3. Flink Checkpoint 机制原理

### 3.1 Checkpoint 是什么？

Checkpoint 是 Flink 实现容错的核心机制，基于 **Chandy-Lamport 算法** 的分布式快照。

**简单理解**：
- Flink 定期给所有算子"拍照"，记录当前的处理进度和状态
- 如果作业失败，从最近一次"照片"的位置恢复
- 类似于游戏的"存档"功能

### 3.2 Checkpoint 执行流程

```
步骤1: JobManager 发送 Checkpoint 屏障（Barrier）
        ┌─────────────────────────────────────────┐
        │              JobManager                  │
        │    "请所有算子做一次快照！"              │
        └──────────┬──────────────┬───────────────┘
                   │              │
步骤2: 屏障随数据流传播        │
        │              │
   ┌────▼────┐   ┌────▼────┐   ┌─────────┐
   │ Source  │──▶│ 算子1   │──▶│ 算子2   │──▶ Sink
   │         │   │         │   │         │
   │ Barrier▶│   │ Barrier▶│   │ Barrier▶│
   └────┬────┘   └────┬────┘   └────┬────┘
        │              │              │
步骤3: 各算子将状态保存到 MinIO
        │              │              │
        ▼              ▼              ▼
   ┌──────────────────────────────────────┐
   │            MinIO 存储                 │
   │  s3://flink-checkpoints/             │
   │    └─ flink/checkpoints/             │
   │        └─ <job-id>/                  │
   │            ├─ chk-1/_metadata        │
   │            └─ chk-1/owned/...        │
   └──────────────────────────────────────┘

步骤4: 所有算子完成 → JobManager 确认 Checkpoint 成功
```

### 3.3 Checkpoint 目录结构

```
s3://flink-checkpoints/flink/checkpoints/
  └── <job-id>/
      ├── chk-1/                          # 第 1 个 Checkpoint
      │   ├── _metadata                   # 元数据（状态引用、算子链信息）
      │   └── owned/                      # 状态实际数据
      │       ├── <operator-id-1>/        # 算子1的状态
      │       │   └── state-1             # RocksDB SST 文件
      │       └── <operator-id-2>/        # 算子2的状态
      │           └── state-1
      ├── chk-2/                          # 第 2 个 Checkpoint
      │   ├── _metadata
      │   └── owned/
      │       └── ...                     # 增量：只包含变化的部分
      └── shared/                         # 共享状态（RocksDB 增量 Checkpoint）
          └── ...
```

### 3.4 Checkpoint 配置参数详解

```java
// Checkpoint 间隔：多久做一次快照
env.enableCheckpointing(30000);  // 30 秒

// Checkpoint 模式
checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// EXACTLY_ONCE: 精确一次，保证数据不丢不重（推荐）
// AT_LEAST_ONCE: 至少一次，可能重复但性能更好

// Checkpoint 超时
checkpointConfig.setCheckpointTimeout(600000);  // 10 分钟
// 如果 Checkpoint 在此时间内未完成，则视为失败

// 最小间隔
checkpointConfig.setMinPauseBetweenCheckpoints(500);  // 500ms
// 两个 Checkpoint 之间的最小间隔，避免过于频繁

// 最大并发
checkpointConfig.setMaxConcurrentCheckpoints(1);
// 同时进行的 Checkpoint 数量，1 表示串行

// 外部化清理策略
checkpointConfig.setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);
// RETAIN_ON_CANCELLATION: 作业取消时保留 Checkpoint（可用于恢复）
// DELETE_ON_CANCELLATION: 作业取消时删除 Checkpoint
```

---

## 4. SeaweedFS vs MinIO 深度对比

### 4.1 架构对比

```
SeaweedFS 架构（3 个组件）:
┌──────────┐   ┌──────────┐   ┌──────────┐
│  Master   │   │  Volume  │   │  Filer   │
│ (元数据)  │   │ (数据存储)│   │ (文件接口)│
└──────────┘   └──────────┘   └──────────┘

MinIO 架构（1 个组件）:
┌──────────────────────┐
│       MinIO           │
│  (S3 兼容对象存储)    │
│  元数据 + 数据存储    │
└──────────────────────┘
```

### 4.2 Checkpoint 存储方式对比

| 方面 | SeaweedFS | MinIO |
|------|-----------|-------|
| 协议 | file://, http://, hdfs:// | s3:// |
| Flink 集成 | 需要挂载或 Hadoop 兼容层 | 原生 S3 插件支持 |
| 配置复杂度 | 高（需要挂载或 Hadoop 配置） | 低（只需 S3 配置） |
| Bucket 管理 | 不需要 | 需要预先创建 |
| 路径风格 | 文件系统路径 | S3 对象路径 |
| 一致性模型 | 最终一致 | 强一致（默认） |

### 4.3 迁移修改点汇总

```
┌────────────────────────────────────────────────────────────┐
│                    迁移修改点清单                            │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  1. pom.xml                                                │
│     + flink-s3-fs-hadoop                                   │
│     + flink-statebackend-rocksdb                           │
│     + minio (可选，用于验证)                                │
│                                                            │
│  2. FlinkConfig.java                                       │
│     + s3.endpoint 配置                                     │
│     + s3.access-key 配置                                   │
│     + s3.secret-key 配置                                   │
│     + s3.path.style.access = true                          │
│     ~ setCheckpointStorage: file:// → s3://                │
│                                                            │
│  3. application.yml                                        │
│     ~ seaweedfs.* → minio.*                                │
│     ~ checkpoint-path: file:// → s3://                     │
│                                                            │
│  4. 运维                                                   │
│     + 创建 MinIO Bucket                                    │
│     + 配置 MinIO 访问策略                                  │
│     + 监控 MinIO 存储使用量                                │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

---

## 5. 测试策略与设计

### 5.1 测试金字塔

```
          ┌─────────────┐
          │  E2E 测试    │  ← 需要完整 Flink 集群 + MinIO
          │ (生产环境)   │
         ┌┴─────────────┴┐
         │  集成测试       │  ← MiniCluster + MinIO Container
         │ (Checkpoint    │
         │  写入验证)      │
        ┌┴───────────────┴┐
        │   单元测试        │  ← MinIO 连接测试、配置测试
        │ (基础功能验证)    │
       ┌┴─────────────────┴┐
       │    编译验证         │  ← mvn compile / test-compile
       │ (代码正确性)       │
       └───────────────────┘
```

### 5.2 测试类详解

#### MinioConnectionTest（单元测试）

**目的**：验证 MinIO 容器启动和基本操作

**测试用例**：

| 测试方法 | 验证内容 |
|----------|----------|
| `testMinioContainerIsRunning` | 容器是否运行、客户端是否可创建 |
| `testCreateBucket` | Bucket 创建是否成功 |
| `testUploadAndListFiles` | 文件上传和列表是否正常 |
| `testCreateBucketIdempotent` | Bucket 创建幂等性 |

**技术要点**：
- 使用 `@Container` 注解自动管理容器生命周期
- Testcontainers 分配随机端口避免冲突
- 每个测试方法前自动执行 `@BeforeEach`

#### CheckpointIntegrationTest（集成测试）

**目的**：验证 Flink Checkpoint 能成功写入 MinIO

**测试用例**：

| 测试方法 | 验证内容 |
|----------|----------|
| `testCheckpointWritesToMinio` | Checkpoint 文件是否出现在 MinIO |
| `testCheckpointContainsMetadata` | Checkpoint 是否包含 `_metadata` 文件 |

**技术要点**：
- 使用 `MiniCluster` 在单 JVM 中运行 Flink 作业
- 手动创建和销毁 MiniCluster（`@BeforeEach` / `@AfterEach`）
- 通过 MinIO Client 验证文件存在性

#### StateRecoveryTest（恢复测试）

**目的**：验证有状态作业的状态管理和恢复能力

**测试用例**：

| 测试方法 | 验证内容 |
|----------|----------|
| `testStatefulJobMaintainsState` | 有状态计数器是否正确累加 |
| `testCheckpointWrittenToMinio` | Checkpoint 是否写入 MinIO |
| `testStateConsistencyAfterRecovery` | 状态一致性验证 |

**技术要点**：
- 使用 `ValueState<Integer>` 维护计数器状态
- `KeyedProcessFunction` 实现有状态处理
- `executeAndCollect()` 收集作业输出进行断言

### 5.3 Testcontainers 工作原理

```
测试启动
  │
  ├── 1. Testcontainers 检测 Docker 环境
  ├── 2. 拉取 MinIO Docker 镜像（首次）
  ├── 3. 启动 MinIO 容器
  │     ├── 映射端口 9000（API）→ 随机端口
  │     └── 映射端口 9001（Console）→ 随机端口
  ├── 4. 等待容器就绪（健康检查）
  ├── 5. 执行测试方法
  │     └── 使用 getMinioEndpoint() 获取实际地址
  └── 6. 测试结束，自动销毁容器
```

### 5.4 MiniCluster 工作原理

```
┌─────────────────────────────────────────┐
│              单个 JVM                     │
│                                          │
│  ┌──────────────────────────────────┐   │
│  │         MiniCluster              │   │
│  │                                  │   │
│  │  ┌────────────┐                 │   │
│  │  │ Dispatcher │ (JobManager)    │   │
│  │  └─────┬──────┘                 │   │
│  │        │                        │   │
│  │  ┌─────▼──────┐                 │   │
│  │  │TaskExecutor│ (TaskManager)   │   │
│  │  │  Slot 1    │                 │   │
│  │  │  Slot 2    │                 │   │
│  │  │  Slot 3    │                 │   │
│  │  │  Slot 4    │                 │   │
│  │  └────────────┘                 │   │
│  └──────────────────────────────────┘   │
│                                          │
└─────────────────────────────────────────┘
```

---

## 6. S3 协议适配细节

### 6.1 为什么 MinIO 兼容 S3？

MinIO 实现了 Amazon S3 API 的完整子集，包括：

| S3 操作 | MinIO 支持 | Flink 使用 |
|---------|-----------|-----------|
| CreateBucket | ✅ | 需要预先创建 |
| PutObject | ✅ | 写入 Checkpoint 数据 |
| GetObject | ✅ | 读取 Checkpoint 数据 |
| ListObjects | ✅ | 列出 Checkpoint 文件 |
| DeleteObject | ✅ | 清理旧 Checkpoint |
| HeadObject | ✅ | 检查文件是否存在 |
| MultipartUpload | ✅ | 大文件分片上传 |

### 6.2 路径风格 vs 虚拟主机风格

**路径风格**（MinIO 使用）：
```
http://minio:9000/flink-checkpoints/flink/checkpoints/chk-1/_metadata
│                │       │                                        │
│                │       └── Bucket 名称                          │
│                └── MinIO 地址                                   │
└── 协议                                                         │
```

**虚拟主机风格**（AWS 使用）：
```
http://flink-checkpoints.s3.amazonaws.com/flink/checkpoints/chk-1/_metadata
│       │                    │                                       │
│       └── Bucket 作为子域名│                                       │
└── 协议                    └── AWS S3 地址                          │
```

**为什么必须设置 `s3.path.style.access=true`？**

因为 Flink 的 S3 插件默认使用虚拟主机风格，会将 Bucket 名称作为子域名。
MinIO 不支持这种风格，必须使用路径风格。

### 6.3 Flink S3 插件选择

Flink 提供两种 S3 文件系统插件：

| 插件 | Artifact | 特点 |
|------|----------|------|
| Hadoop-based | `flink-s3-fs-hadoop` | 基于 Hadoop FileSystem，功能完整 |
| Presto-based | `flink-s3-fs-presto` | 基于 Presto，更轻量 |

本项目使用 `flink-s3-fs-hadoop`，因为：
1. 功能更完整，支持所有 S3 特性
2. 与 Hadoop 生态兼容
3. 社区推荐用于 Checkpoint 存储

---

## 7. 生产环境部署建议

### 7.1 MinIO 部署

#### 单节点部署（开发/测试）

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=your-access-key \
  -e MINIO_ROOT_PASSWORD=your-secret-key \
  -v minio-data:/data \
  minio/minio:RELEASE.2024-01-16T16-07-38Z server /data --console-address ":9001"
```

#### 多节点部署（生产）

```yaml
# docker-compose.yml
version: '3'
services:
  minio1:
    image: minio/minio:RELEASE.2024-01-16T16-07-38Z
    command: server http://minio{1...4}/data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: your-access-key
      MINIO_ROOT_PASSWORD: your-secret-key
    volumes:
      - minio1-data:/data

  minio2:
    image: minio/minio:RELEASE.2024-01-16T16-07-38Z
    command: server http://minio{1...4}/data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: your-access-key
      MINIO_ROOT_PASSWORD: your-secret-key
    volumes:
      - minio2-data:/data

  # minio3, minio4 类似...
```

### 7.2 安全配置

```yaml
# 生产环境 application.yml
minio:
  # 使用内部网络地址
  endpoint: http://minio.internal:9000

  # 使用强密码
  access-key: ${MINIO_ACCESS_KEY}   # 从环境变量读取
  secret-key: ${MINIO_SECRET_KEY}   # 从环境变量读取

  bucket: flink-checkpoints-prod
  checkpoint-path: flink/checkpoints
```

### 7.3 Bucket 策略

```bash
# 设置 Bucket 只允许特定 IP 访问
mc admin policy set local readwrite user=flink-service

# 启用 Bucket 版本控制（防止误删）
mc version enable local/flink-checkpoints-prod

# 设置生命周期规则（自动清理旧 Checkpoint）
mc ilm rule add local/flink-checkpoints-prod --expire-days 7
```

### 7.4 监控指标

| 指标 | 说明 | 告警阈值 |
|------|------|----------|
| Bucket 大小 | Checkpoint 存储总量 | > 100GB |
| Checkpoint 耗时 | 单次 Checkpoint 完成时间 | > 60s |
| Checkpoint 失败率 | 失败次数/总次数 | > 10% |
| MinIO 磁盘使用率 | 存储节点磁盘使用 | > 80% |

---

## 8. 性能优化指南

### 8.1 Checkpoint 性能优化

#### 增量 Checkpoint

```java
// 启用增量 Checkpoint（只保存变化部分）
EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
//                                                                  ^^^^
//                                                        true = 启用增量
```

**效果**：
- 全量 Checkpoint：每次保存全部状态（可能几 GB）
- 增量 Checkpoint：只保存上次 Checkpoint 后的变化（可能几 MB）

#### 调整 Checkpoint 间隔

```java
// 间隔太短：影响作业性能
env.enableCheckpointing(1000);   // 1秒 - 太频繁

// 间隔太长：恢复时丢失更多数据
env.enableCheckpointing(300000); // 5分钟 - 太稀疏

// 推荐：根据业务容忍的数据丢失量决定
env.enableCheckpointing(30000);  // 30秒 - 平衡选择
```

#### 并发 Checkpoint

```java
// 允许并发 Checkpoint（适用于 Checkpoint 耗时较长的场景）
checkpointConfig.setMaxConcurrentCheckpoints(2);
// 注意：会增加 MinIO 的写入压力
```

### 8.2 MinIO 性能优化

#### 分片上传配置

```java
// 大文件分片上传，提高吞吐量
flinkConfig.setString("s3.upload.part.size", "104857600"); // 100MB
flinkConfig.setString("s3.multipart.upload.threshold", "20971520"); // 20MB
```

#### 连接池配置

```java
// 增加最大连接数
flinkConfig.setString("s3.connection.maximum", "1000");
// 连接超时
flinkConfig.setString("s3.connection.timeout", "10000"); // 10秒
// Socket 超时
flinkConfig.setString("s3.socket.timeout", "120000"); // 2分钟
```

### 8.3 RocksDB 性能优化

```java
// 通过 Flink 配置调整 RocksDB 参数
Configuration config = new Configuration();

// 增加 RocksDB 写缓冲区
config.setString("state.backend.rocksdb.writebuffer.size", "64MB");

// 增加 RocksDB 写缓冲区数量
config.setString("state.backend.rocksdb.writebuffer.count", "4");

env.configure(config);
```

---

## 附录

### A. 完整的 Checkpoint 配置参考

```java
// === 状态后端 ===
EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
env.setStateBackend(stateBackend);

// === S3 配置 ===
Configuration flinkConfig = new Configuration();
flinkConfig.setString("s3.endpoint", "http://minio:9000");
flinkConfig.setString("s3.access-key", "your-access-key");
flinkConfig.setString("s3.secret-key", "your-secret-key");
flinkConfig.setString("s3.path.style.access", "true");
flinkConfig.setString("s3.connection.maximum", "500");
flinkConfig.setString("s3.connection.timeout", "5000");
flinkConfig.setString("s3.socket.timeout", "50000");
flinkConfig.setString("s3.upload.part.size", "104857600");
flinkConfig.setString("s3.multipart.upload.threshold", "20971520");
env.getConfig().setGlobalJobParameters(flinkConfig);

// === Checkpoint 配置 ===
env.enableCheckpointing(30000);
CheckpointConfig cpConfig = env.getCheckpointConfig();
cpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
cpConfig.setCheckpointTimeout(600000);
cpConfig.setMinPauseBetweenCheckpoints(500);
cpConfig.setMaxConcurrentCheckpoints(1);
cpConfig.setCheckpointStorage("s3://flink-checkpoints/flink/checkpoints");
cpConfig.setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);
cpConfig.setTolerableCheckpointFailureNumber(3);

// === 重启策略 ===
env.setRestartStrategy(
    RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS))
);
```

### B. 从 SeaweedFS 迁移的完整代码对比

#### 迁移前（SeaweedFS）

```java
@Configuration
public class FlinkSeaweedfsConfig {

    @Value("${seaweedfs.mount-path}")
    private String mountPath;

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // SeaweedFS: 使用 HashMapStateBackend
        env.setStateBackend(new HashMapStateBackend());

        // SeaweedFS: 不需要 S3 配置

        // SeaweedFS: Checkpoint 存储到挂载目录
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointStorage(
            "file://" + mountPath + "/flink/checkpoints"
        );

        return env;
    }
}
```

#### 迁移后（MinIO）

```java
@Configuration
public class FlinkMinioConfig {

    @Autowired
    private MinioProperties minioProperties;

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // MinIO: 使用 RocksDB 状态后端（支持增量 Checkpoint）
        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(stateBackend);

        // MinIO: 必须配置 S3 连接参数
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("s3.endpoint", minioProperties.getEndpoint());
        flinkConfig.setString("s3.access-key", minioProperties.getAccessKey());
        flinkConfig.setString("s3.secret-key", minioProperties.getSecretKey());
        flinkConfig.setString("s3.path.style.access", "true");
        env.getConfig().setGlobalJobParameters(flinkConfig);

        // MinIO: Checkpoint 存储到 S3 路径
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointStorage(
            minioProperties.getFullCheckpointPath()  // s3://bucket/path
        );

        return env;
    }
}
```

### C. 故障排查流程图

```
Checkpoint 失败
    │
    ├── MinIO 连接问题？
    │   ├── 检查 s3.endpoint 是否正确
    │   ├── 检查 MinIO 是否运行
    │   └── 检查网络连通性
    │
    ├── 权限问题？
    │   ├── 检查 access-key / secret-key
    │   ├── 检查 Bucket 是否存在
    │   └── 检查 s3.path.style.access = true
    │
    ├── 超时问题？
    │   ├── 增加 checkpoint-timeout
    │   ├── 启用增量 Checkpoint
    │   └── 检查 MinIO 性能
    │
    └── S3 插件问题？
        ├── 确认 flink-s3-fs-hadoop 依赖
        ├── 检查 Flink 版本兼容性
        └── 查看 Flink 日志
```
