# Flink Checkpoint 从 SeaweedFS 迁移到 MinIO 指南

> 一个完整的 Spring Boot 3.5.6 + Flink 1.18 项目，演示如何将 Flink Checkpoint 存储从 SeaweedFS 迁移到 MinIO。

***

## 目录

1. [项目背景](#1-项目背景)
2. [核心概念](#2-核心概念)
3. [环境准备](#3-环境准备)
4. [项目快速启动](#4-项目快速启动)
5. [代码结构说明](#5-代码结构说明)
6. [迁移指南](#6-迁移指南)
7. [测试说明](#7-测试说明)
8. [常见问题排查](#8-常见问题排查)

***

## 1. 项目背景

### 1.1 什么是 Flink Checkpoint？

**Flink Checkpoint** 是 Apache Flink 的容错机制核心。简单来说：

- **快照**：Flink 定期给正在运行的流处理作业"拍一张照片"，记录当前的处理进度和中间状态
- **恢复**：如果作业崩溃了，Flink 可以从最近一张"照片"的位置继续处理，而不是从头开始
- **类比**：就像游戏中的"存档"功能，死了可以从存档点复活

**Checkpoint 的工作流程**：

```
数据流 → [算子1] → [算子2] → [算子3] → 输出
              ↓         ↓         ↓
           状态1      状态2      状态3
              ↓         ↓         ↓
           ┌─────────────────────────┐
           │    Checkpoint 存储       │
           │  (SeaweedFS / MinIO)    │
           └─────────────────────────┘
```

### 1.2 什么是 SeaweedFS？

**SeaweedFS** 是一个分布式文件系统，特点是：

- 适合存储大量小文件
- 支持 S3 兼容接口（通过 S3 Gateway）
- 支持 Filer 模式（类似传统文件系统）
- 可以作为 Flink Checkpoint 的存储后端

### 1.3 什么是 MinIO？

**MinIO** 是一个高性能的对象存储系统，特点是：

- 完全兼容 Amazon S3 API
- 部署简单，单二进制文件即可运行
- 性能优秀，适合大规模数据存储
- 提供 Web 管理界面（Console）

### 1.4 为什么要从 SeaweedFS 迁移到 MinIO？

| 对比项           | SeaweedFS                  | MinIO               |
| ------------- | -------------------------- | ------------------- |
| S3 兼容性        | 部分兼容（S3 Gateway）           | 完全兼容                |
| 部署复杂度         | 需要 Master + Volume + Filer | 单节点即可               |
| 管理界面          | 基础                         | 完整的 Web Console     |
| 生态集成          | 需要额外适配                     | 原生 S3 协议，Flink 直接支持 |
| Checkpoint 路径 | `file://` 或 `hdfs://`      | `s3://`             |
| 社区活跃度         | 中等                         | 高                   |

**核心原因**：MinIO 完全兼容 S3 协议，Flink 的 S3 文件系统插件可以直接使用，无需额外适配。

***

## 2. 核心概念

### 2.1 Flink 状态后端

状态后端决定了 Flink 如何存储和管理作业的状态：

| 状态后端                        | 存储位置       | 适用场景 | 增量 Checkpoint |
| --------------------------- | ---------- | ---- | ------------- |
| HashMapStateBackend         | JVM 堆内存    | 小状态  | 不支持           |
| EmbeddedRocksDBStateBackend | 本地 RocksDB | 大状态  | 支持            |

**本项目使用 RocksDB 状态后端**，因为生产环境通常状态较大，且支持增量 Checkpoint（减少写入 MinIO 的数据量）。

### 2.2 Checkpoint 存储路径

Checkpoint 存储路径决定了快照数据保存在哪里：

```
SeaweedFS（挂载模式）：file:///mnt/seaweedfs/flink/checkpoints
SeaweedFS（Hadoop模式）：hdfs://seaweedfs:9200/flink/checkpoints
MinIO（S3协议）：s3://flink-checkpoints/flink/checkpoints
```

### 2.3 S3 协议与 MinIO

MinIO 兼容 S3 协议，Flink 通过 S3 文件系统插件访问 MinIO：

```
Flink 作业 → S3 插件 → MinIO（模拟 AWS S3）
```

关键配置项：

- `s3.endpoint`：MinIO 地址（而非 AWS 地址）
- `s3.access-key` / `s3.secret-key`：MinIO 的访问密钥
- `s3.path.style.access`：设为 `true`（MinIO 使用路径风格）

***

## 3. 环境准备

### 3.1 必备软件

| 软件     | 版本要求 | 安装方式                                          |
| ------ | ---- | --------------------------------------------- |
| JDK    | 21+  | [Adoptium](https://adoptium.net/)             |
| Maven  | 3.8+ | [maven.apache.org](https://maven.apache.org/) |
| Docker | 20+  | [docker.com](https://www.docker.com/)         |

### 3.2 启动 MinIO（Docker 方式）

```bash
# 拉取 MinIO 镜像
docker pull minio/minio:RELEASE.2024-01-16T16-07-38Z

# 启动 MinIO
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:RELEASE.2024-01-16T16-07-38Z server /data --console-address ":9001"
```

启动后：

- **API 地址**：<http://localhost:9000>
- **管理界面**：<http://localhost:9001>
- **用户名**：minioadmin
- **密码**：minioadmin

### 3.3 创建 Bucket

在 MinIO Console（<http://localhost:9001）中：>

1. 登录（minioadmin / minioadmin）
2. 点击 "Buckets" → "Create Bucket"
3. 输入名称 `flink-checkpoints`
4. 点击 "Create Bucket"

或者使用 MinIO Client（mc）：

```bash
# 安装 mc
docker run --rm minio/mc:RELEASE.2024-01-16T16-07-38Z --help

# 配置 mc 连接本地 MinIO
mc alias set local http://localhost:9000 minioadmin minioadmin

# 创建 Bucket
mc mb local/flink-checkpoints
```

***

## 4. 项目快速启动

### 4.1 克隆项目

```bash
cd your-workspace
# 项目已在当前目录
cd flink-minio-migration
```

### 4.2 修改配置

编辑 `src/main/resources/application.yml`：

```yaml
minio:
  endpoint: http://localhost:9000    # MinIO 地址
  access-key: minioadmin             # 访问密钥
  secret-key: minioadmin             # 密钥
  bucket: flink-checkpoints          # Bucket 名称
  checkpoint-path: flink/checkpoints # Checkpoint 路径
```

### 4.3 编译项目

```bash
mvn clean compile
```

### 4.4 运行项目

```bash
mvn spring-boot:run
```

### 4.5 提交 Flink 作业

```bash
# 使用内置数据源提交 WordCount 作业
curl -X POST http://localhost:8080/api/flink/job/submit

# 使用指定文件路径提交作业
curl -X POST "http://localhost:8080/api/flink/job/submit?inputFilePath=s3://flink-checkpoints/input/words.txt"
```

### 4.6 查看 Checkpoint 状态

```bash
# 检查 Checkpoint 是否存在
curl http://localhost:8080/api/flink/checkpoint/exists

# 列出 Checkpoint 文件
curl http://localhost:8080/api/flink/checkpoint/list

# 查看配置信息
curl http://localhost:8080/api/flink/config
```

### 4.7 观察 Checkpoint 输出

在 MinIO Console（<http://localhost:9001）中：>

1. 打开 `flink-checkpoints` Bucket
2. 导航到 `flink/checkpoints` 目录
3. 可以看到 Flink 创建的 Checkpoint 文件

Checkpoint 目录结构：

```
flink/checkpoints/
  └── <job-id>/
      ├── chk-1/
      │   ├── _metadata           # Checkpoint 元数据
      │   └── owned/              # 状态文件
      ├── chk-2/
      │   ├── _metadata
      │   └── owned/
      └── ...
```

***

## 5. 代码结构说明

```
flink-minio-migration/
├── pom.xml                                    # Maven 构建配置
├── src/
│   ├── main/
│   │   ├── java/com/example/flinkminio/
│   │   │   ├── FlinkMinioApplication.java     # Spring Boot 启动类
│   │   │   ├── config/
│   │   │   │   ├── FlinkConfig.java           # 【核心】Flink 执行环境配置
│   │   │   │   ├── MinioProperties.java       # MinIO 连接配置属性
│   │   │   │   ├── SeaweedFsProperties.java   # SeaweedFS 连接配置属性
│   │   │   │   └── MinioClientConfig.java     # MinIO 客户端 Bean
│   │   │   ├── job/
│   │   │   │   └── WordCountJob.java          # Flink 流处理作业
│   │   │   ├── service/
│   │   │   │   ├── CheckpointService.java     # Checkpoint 管理服务
│   │   │   │   ├── MinioInitService.java      # MinIO 初始化服务
│   │   │   │   └── MigrationVerifyService.java # 【迁移验证】SeaweedFS vs MinIO 对比
│   │   │   ├── controller/
│   │   │   │   └── JobController.java         # REST API 控制器
│   │   │   └── model/
│   │   │       └── JobSubmissionResult.java   # 作业提交结果模型
│   │   └── resources/
│   │       └── application.yml                # 应用配置文件
│   └── test/
│       ├── java/com/example/flinkminio/
│       │   ├── FlinkMinioApplicationTest.java # 基础测试
│       │   ├── checkpoint/
│       │   │   ├── MinioConnectionTest.java   # MinIO 连接测试
│       │   │   ├── CheckpointIntegrationTest.java # Checkpoint 集成测试
│       │   │   └── StateRecoveryTest.java     # 状态恢复测试
│       │   └── util/
│       │       └── MinioTestContainer.java    # MinIO 测试容器工具
│       └── resources/
│           └── application-test.yml           # 测试配置
└── README.md                                  # 本文档
```

### 5.1 关键类说明

#### FlinkConfig.java（最核心的配置类）

这是迁移的核心文件，包含了所有从 SeaweedFS 迁移到 MinIO 需要修改的配置：

```java
// 1. 状态后端：使用 RocksDB（支持增量 Checkpoint）
EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
env.setStateBackend(rocksDBStateBackend);

// 2. S3 配置：连接 MinIO（迁移的关键）
flinkConfig.setString("s3.endpoint", minioProperties.getEndpoint());
flinkConfig.setString("s3.access-key", minioProperties.getAccessKey());
flinkConfig.setString("s3.secret-key", minioProperties.getSecretKey());
flinkConfig.setString("s3.path.style.access", "true");

// 3. Checkpoint 存储：使用 S3 路径（迁移的核心修改点）
checkpointConfig.setCheckpointStorage("s3://flink-checkpoints/flink/checkpoints");
```

#### WordCountJob.java（Flink 作业示例）

一个简单的流处理作业，演示 Flink 的基本使用：

- 从数据源读取文本
- 按单词分组统计频率
- 使用滚动窗口聚合

#### CheckpointService.java（Checkpoint 管理）

提供 Checkpoint 的查询和验证功能：

- 提交 Flink 作业
- 列出 MinIO 中的 Checkpoint 文件
- 检查 Checkpoint 是否存在

#### MigrationVerifyService.java（迁移验证服务）⭐

这是迁移验证的核心服务，提供 SeaweedFS → MinIO 的对比验证能力：

- `verifySeaweedFs(path)`：验证 SeaweedFS 挂载路径
- `verifySeaweedFsFiler(filerUrl, checkpointPath)`：验证 SeaweedFS Filer（HTTP API 模式）
- `verifySeaweedFsFromConfig()`：使用配置文件验证 SeaweedFS
- `verifyMinio()`：验证新 MinIO 存储（迁移后验收测试）
- `compareAndVerify(seaweedFsPath)`：完整迁移对比验证，给出 6 项标准判定
- `initMinioBucket()`：初始化 MinIO Bucket

**迁移成功的 6 项标准**：

| 编号 | 标准 | 类型 |
|------|------|------|
| 1 | MinIO 连接正常 | 核心 |
| 2 | MinIO Bucket 存在 | 核心 |
| 3 | MinIO 中存在 Checkpoint | 核心 |
| 4 | Checkpoint 含 _metadata | 增强 |
| 5 | Checkpoint 文件数 > 0 | 增强 |
| 6 | MinIO 可替代 SeaweedFS | 增强 |

***

## 6. 迁移指南

### 6.1 迁移概览

从 SeaweedFS 迁移到 MinIO 的核心修改点：

```
┌─────────────────────────────────────────────────────────┐
│                    迁移修改点                             │
├─────────────────────────────────────────────────────────┤
│ 1. 依赖：添加 flink-s3-fs-hadoop                        │
│ 2. 配置：添加 S3 endpoint / access-key / secret-key     │
│ 3. 路径：file:// 或 hdfs:// → s3://                     │
│ 4. Bucket：预先创建 MinIO Bucket                         │
│ 5. 验证：确认 Checkpoint 成功写入 MinIO                  │
└─────────────────────────────────────────────────────────┘
```

### 6.2 第一步：修改 Maven 依赖

**迁移前（SeaweedFS）**：

```xml
<!-- SeaweedFS 通常不需要额外依赖 -->
<!-- 如果使用 Hadoop 接口，可能需要： -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>3.3.6</version>
</dependency>
```

**迁移后（MinIO）**：

```xml
<!-- 添加 Flink S3 文件系统插件（必须） -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-s3-fs-hadoop</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- 添加 RocksDB 状态后端（推荐） -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- MinIO Java 客户端（可选，用于验证） -->
<dependency>
    <groupId>io.minio</groupId>
    <artifactId>minio</artifactId>
    <version>8.5.9</version>
</dependency>
```

### 6.3 第二步：修改 Checkpoint 存储路径

**迁移前（SeaweedFS 挂载模式）**：

```java
// SeaweedFS 通过操作系统挂载为本地目录
// Checkpoint 路径使用 file:// 协议
checkpointConfig.setCheckpointStorage("file:///mnt/seaweedfs/flink/checkpoints");
```

**迁移前（SeaweedFS Hadoop 模式）**：

```java
// SeaweedFS 通过 Hadoop 兼容接口
// Checkpoint 路径使用 hdfs:// 协议
checkpointConfig.setCheckpointStorage("hdfs://seaweedfs-namenode:9200/flink/checkpoints");
```

**迁移后（MinIO S3 模式）**：

```java
// MinIO 兼容 S3 协议
// Checkpoint 路径使用 s3:// 协议
checkpointConfig.setCheckpointStorage("s3://flink-checkpoints/flink/checkpoints");
```

### 6.4 第三步：添加 S3 连接配置

**迁移前（SeaweedFS）**：

```java
// SeaweedFS 挂载模式不需要额外配置
// 操作系统层面已经处理了连接

// SeaweedFS Hadoop 模式需要配置 Hadoop core-site.xml
// 或者在代码中设置 Hadoop 配置
```

**迁移后（MinIO）**：

```java
// 必须配置 S3 连接参数，指向 MinIO
Configuration flinkConfig = new Configuration();

// S3 endpoint：指向 MinIO 地址（而非 AWS）
flinkConfig.setString("s3.endpoint", "http://localhost:9000");

// S3 访问密钥
flinkConfig.setString("s3.access-key", "minioadmin");
flinkConfig.setString("s3.secret-key", "minioadmin");

// 路径风格访问（MinIO 必须设为 true）
flinkConfig.setString("s3.path.style.access", "true");

// 注册到 Flink 执行环境
env.getConfig().setGlobalJobParameters(flinkConfig);
```

### 6.5 第四步：修改配置文件

**迁移前（SeaweedFS application.yml）**：

```yaml
seaweedfs:
  filer-host: seaweedfs-filer
  filer-port: 8888
  mount-path: /mnt/seaweedfs
  checkpoint-path: file:///mnt/seaweedfs/flink/checkpoints
```

**迁移后（MinIO application.yml）**：

```yaml
minio:
  endpoint: http://localhost:9000
  access-key: minioadmin
  secret-key: minioadmin
  bucket: flink-checkpoints
  checkpoint-path: flink/checkpoints
```

### 6.6 第五步：创建 MinIO Bucket

迁移后需要预先创建 Bucket：

```bash
# 使用 mc 命令行工具
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/flink-checkpoints

# 或在代码中自动创建（参见 MinioInitService.java）
```

### 6.7 迁移检查清单

- [ ] 添加 `flink-s3-fs-hadoop` 依赖
- [ ] 添加 `flink-statebackend-rocksdb` 依赖（推荐）
- [ ] 配置 `s3.endpoint` 指向 MinIO 地址
- [ ] 配置 `s3.access-key` 和 `s3.secret-key`
- [ ] 配置 `s3.path.style.access` 为 `true`
- [ ] 将 Checkpoint 路径从 `file://` 或 `hdfs://` 改为 `s3://`
- [ ] 在 MinIO 中创建 Bucket
- [ ] 运行作业验证 Checkpoint 写入成功
- [ ] 测试从 Checkpoint 恢复

### 6.8 第六步：迁移验证（关键！）

迁移配置修改完成后，**必须**进行系统性的验证，确认 MinIO 能完全替代 SeaweedFS。

#### 步骤 1：迁移前 — 记录 SeaweedFS 基准

```bash
# 验证 SeaweedFS 当前状态，记录基准值
curl "http://localhost:8080/api/flink/migration/verify-seaweedfs?path=/mnt/seaweedfs/flink/checkpoints"
# 记录返回的 checkpointFileCount 和 totalCheckpointSize
```

#### 步骤 2：迁移后 — 验证 MinIO 可用

```bash
# 初始化 Bucket
curl -X POST http://localhost:8080/api/flink/migration/init-bucket

# 提交作业触发 Checkpoint
curl -X POST http://localhost:8080/api/flink/job/submit
sleep 30

# 验证 MinIO
curl http://localhost:8080/api/flink/migration/verify-minio
```

#### 步骤 3：对比验证 — 综合判定

```bash
# 一键对比验证
curl "http://localhost:8080/api/flink/migration/compare?seaweedFsPath=/mnt/seaweedfs/flink/checkpoints"
# 检查 migrationSuccessful = true
```

> 详细的迁移验证标准和判定规则，请参见 [7.5 迁移验证：如何确认迁移成功](#75-迁移验证如何确认迁移成功)。

***

## 7. 测试说明与迁移验证

### 7.1 测试架构

```
┌──────────────────────────────────────────────────────────────┐
│                        测试层次                                │
├──────────────────────────────────────────────────────────────┤
│ 第一层：MinIO 连接测试（MinioConnectionTest）                   │
│   └── 验证 MinIO 容器启动和基本操作                              │
│                                                              │
│ 第二层：Checkpoint 集成测试（CheckpointIntegrationTest）        │
│   └── MiniCluster + MinIO Container                          │
│   └── 验证 Flink Checkpoint 写入 MinIO                        │
│                                                              │
│ 第三层：状态恢复测试（StateRecoveryTest）                        │
│   └── 验证有状态作业的状态管理和恢复                               │
│                                                              │
│ 第四层：迁移对比验证（MigrationVerifyService + REST API）        │
│   └── SeaweedFS vs MinIO 存储对比                             │
│   └── 迁移成功判定                                             │
└──────────────────────────────────────────────────────────────┘
```

### 7.2 运行测试

```bash
# 运行所有测试
mvn test

# 只运行 MinIO 连接测试
mvn test -Dtest=MinioConnectionTest

# 只运行 Checkpoint 集成测试
mvn test -Dtest=CheckpointIntegrationTest

# 只运行状态恢复测试
mvn test -Dtest=StateRecoveryTest

# 跳过测试直接打包
mvn clean package -DskipTests
```

### 7.3 测试类详解

#### 7.3.1 MinIO 连接测试（MinioConnectionTest）

验证 MinIO 容器是否正常启动，以及基本的 Bucket 操作：

| 测试方法 | 验证内容 | 通过条件 |
|----------|----------|----------|
| `testMinioContainerIsRunning` | 容器启动 + 客户端创建 | 容器状态=running，客户端非null |
| `testCreateBucket` | Bucket 创建 | `bucketExists()` 返回 true |
| `testUploadAndListFiles` | 文件上传和列表 | 上传后列表非空，包含上传的文件 |
| `testCreateBucketIdempotent` | Bucket 创建幂等性 | 重复创建不报错 |

**关键代码解读**：

```java
// Testcontainers 自动管理 MinIO 容器生命周期
@Container
static MinIOContainer minioContainer = new MinIOContainer(
    DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z")
).withUserName("minioadmin").withPassword("minioadmin");

// 从容器获取实际连接地址（随机端口）
String endpoint = minioContainer.getS3URL();
```

#### 7.3.2 Checkpoint 集成测试（CheckpointIntegrationTest）

使用 Flink MiniCluster + Testcontainers MinIO，验证 Checkpoint 完整写入流程：

| 测试方法 | 验证内容 | 通过条件 |
|----------|----------|----------|
| `testCheckpointWritesToMinio` | Checkpoint 写入 MinIO | MinIO 中存在 chk-* 目录 |
| `testCheckpointContainsMetadata` | Checkpoint 元数据完整 | 存在 `_metadata` 文件 |

**关键代码解读**：

```java
// 创建 MiniCluster（单 JVM 内的 Flink 集群）
MiniClusterConfiguration clusterConfig = new MiniClusterConfiguration.Builder()
    .setNumTaskManagers(1)
    .setNumSlotsPerTaskManager(4)
    .setConfiguration(config)
    .build();
MiniCluster miniCluster = new MiniCluster(clusterConfig);
miniCluster.start();

// 配置 S3 指向 Testcontainers MinIO
flinkConfig.setString("s3.endpoint", minioContainer.getS3URL());
flinkConfig.setString("s3.path.style.access", "true");
```

#### 7.3.3 状态恢复测试（StateRecoveryTest）

验证有状态作业的状态管理和恢复能力：

| 测试方法 | 验证内容 | 通过条件 |
|----------|----------|----------|
| `testStatefulJobMaintainsState` | 有状态计数器累加 | 输出计数递增 |
| `testCheckpointWrittenToMinio` | Checkpoint 写入 MinIO | MinIO 中存在文件 |
| `testStateConsistencyAfterRecovery` | 状态一致性 | 恢复后计数从断点继续 |

**关键代码解读**：

```java
// 使用 ValueState 维护有状态计数器
public static class StatefulCounter extends KeyedProcessFunction<String, String, String> {
    private ValueState<Integer> countState;

    @Override
    public void open(Configuration parameters) {
        // 定义状态描述符
        countState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Integer.class)
        );
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        Integer current = countState.value();
        if (current == null) current = 0;
        current++;
        countState.update(current);
        out.collect(value + ": " + current);
    }
}
```

### 7.4 测试前提

运行测试需要：

1. **Docker** 已安装并运行（Testcontainers 需要）
2. **JDK 21+**
3. **Maven 3.8+**

> 注意：Testcontainers 会自动拉取 MinIO Docker 镜像，首次运行可能较慢。

---

### 7.5 迁移验证：如何确认迁移成功

这是整个项目最关键的环节。迁移不是简单的"改个配置"，需要系统性地验证新存储能完全替代旧存储。

#### 7.5.1 验证原 SeaweedFS 正常（迁移前基准测试）

**目的**：确认迁移前旧存储工作正常，建立基准数据。

**SeaweedFS 有三种访问模式**：

| 模式 | 路径格式 | 验证接口 |
|------|----------|----------|
| 挂载模式 | `file:///mnt/seaweedfs/...` | `/verify-seaweedfs?path=...` |
| Filer HTTP API | `http://filer:8888/...` | `/verify-seaweedfs-filer?filerUrl=...` |
| 配置文件模式 | application.yml 配置 | `/verify-seaweedfs-config` |

**方法一：Filer HTTP API 验证（推荐）**

大多数生产环境使用 Filer HTTP API 模式：

```bash
# 验证 SeaweedFS Filer（HTTP API 模式）
curl "http://localhost:8080/api/flink/migration/verify-seaweedfs-filer?filerUrl=http://seaweedfs-dev-filer.geip-xa-dev-gdmp:8888&checkpointPath=/flink/checkpoints"
```

**方法二：配置文件验证**

在 `application.yml` 中配置 SeaweedFS：

```yaml
seaweedfs:
  enabled: true
  filer-url: http://seaweedfs-dev-filer.geip-xa-dev-gdmp:8888
  checkpoint-path: /flink/checkpoints
```

然后调用：

```bash
# 使用配置文件中的 SeaweedFS 信息验证
curl http://localhost:8080/api/flink/migration/verify-seaweedfs-config
```

**方法三：挂载路径验证**

如果 SeaweedFS 已挂载为本地目录：

```bash
# 验证 SeaweedFS 挂载路径可访问
curl "http://localhost:8080/api/flink/migration/verify-seaweedfs?path=/mnt/seaweedfs/flink/checkpoints"
```

**预期返回**：

```json
{
  "storage": "SeaweedFS",
  "path": "/mnt/seaweedfs/flink/checkpoints",
  "accessible": true,
  "checkpointExists": true,
  "checkpointFileCount": 12,
  "totalCheckpointSize": 5242880,
  "totalCheckpointSizeHuman": "5.0 MB"
}
```

**判断标准**：

| 指标 | 期望值 | 说明 |
|------|--------|------|
| `accessible` | `true` | SeaweedFS 路径可访问 |
| `checkpointExists` | `true` | 存在历史 Checkpoint |
| `checkpointFileCount` | > 0 | Checkpoint 文件数量 |
| `totalCheckpointSize` | > 0 | Checkpoint 总大小 |

**方法二：手动验证**

```bash
# 检查 SeaweedFS 挂载点
ls -la /mnt/seaweedfs/flink/checkpoints/

# 检查 Checkpoint 目录
ls /mnt/seaweedfs/flink/checkpoints/*/chk-*/

# 检查 _metadata 文件
find /mnt/seaweedfs/flink/checkpoints -name "_metadata" | head -5
```

**记录基准值**：将 `checkpointFileCount` 和 `totalCheckpointSize` 记录下来，作为迁移后的对比基准。

#### 7.5.2 验证新 MinIO 正常（迁移后验收测试）

**目的**：确认迁移后新存储工作正常，Checkpoint 能成功写入 MinIO。

**方法一：REST API 验证**

```bash
# 第一步：初始化 MinIO Bucket
curl -X POST http://localhost:8080/api/flink/migration/init-bucket

# 第二步：提交 Flink 作业（触发 Checkpoint）
curl -X POST http://localhost:8080/api/flink/job/submit

# 等待 30 秒（等待至少一次 Checkpoint 完成）
sleep 30

# 第三步：验证 MinIO 存储
curl http://localhost:8080/api/flink/migration/verify-minio
```

**预期返回**：

```json
{
  "storage": "MinIO",
  "endpoint": "http://localhost:9000",
  "accessible": true,
  "bucketExists": true,
  "checkpointExists": true,
  "hasMetadata": true,
  "checkpointFileCount": 8,
  "totalCheckpointSize": 3145728,
  "totalCheckpointSizeHuman": "3.0 MB"
}
```

**判断标准**：

| 指标 | 期望值 | 说明 |
|------|--------|------|
| `accessible` | `true` | MinIO 连接正常 |
| `bucketExists` | `true` | Bucket 已创建 |
| `checkpointExists` | `true` | Checkpoint 已写入 |
| `hasMetadata` | `true` | 元数据完整（最关键！） |
| `checkpointFileCount` | > 0 | Checkpoint 文件数量 |
| `totalCheckpointSize` | > 0 | Checkpoint 总大小 |

**方法二：MinIO Console 验证**

1. 打开 http://localhost:9001
2. 登录（minioadmin / minioadmin）
3. 进入 `flink-checkpoints` Bucket
4. 导航到 `flink/checkpoints` 目录
5. 确认存在 `chk-*` 目录和 `_metadata` 文件

**方法三：mc 命令行验证**

```bash
# 配置 mc
mc alias set local http://localhost:9000 minioadmin minioadmin

# 列出 Checkpoint 文件
mc ls local/flink-checkpoints/flink/checkpoints/ --recursive

# 检查 _metadata 文件
mc ls local/flink-checkpoints/flink/checkpoints/ --recursive | grep _metadata
```

#### 7.5.3 对比验证：如何判断迁移成功

**核心接口**：一次性完成 SeaweedFS + MinIO 的对比验证

```bash
# 完整迁移对比验证（含 SeaweedFS 基准）
curl "http://localhost:8080/api/flink/migration/compare?seaweedFsPath=/mnt/seaweedfs/flink/checkpoints"

# 仅验证 MinIO（不验证 SeaweedFS）
curl "http://localhost:8080/api/flink/migration/compare"
```

**迁移成功的 6 项标准**：

| 编号 | 标准 | 类型 | 说明 |
|------|------|------|------|
| 1 | MinIO 连接正常 | 核心 | `accessible = true` |
| 2 | MinIO Bucket 存在 | 核心 | `bucketExists = true` |
| 3 | MinIO 中存在 Checkpoint | 核心 | `checkpointExists = true` |
| 4 | Checkpoint 含 _metadata | 增强 | `hasMetadata = true` |
| 5 | Checkpoint 文件数 > 0 | 增强 | `checkpointFileCount > 0` |
| 6 | MinIO 可替代 SeaweedFS | 增强 | 标准 1-2 通过即可 |

**判定规则**：
- ✅ **迁移成功**：核心标准（1-3）全部通过
- ⚠️ **部分成功**：核心标准通过，增强标准未全部通过（可用但需优化）
- ❌ **迁移失败**：任一核心标准未通过

**预期返回示例（迁移成功）**：

```json
{
  "migrationSuccessful": true,
  "recommendation": "🎉 迁移验证通过！MinIO 已成功替代 SeaweedFS 作为 Flink Checkpoint 存储。建议后续：1) 运行完整业务作业验证；2) 测试从 Checkpoint 恢复；3) 监控 Checkpoint 延迟。",
  "successCriteria": [
    "✅ 标准1通过：MinIO 连接正常（endpoint: http://localhost:9000）",
    "✅ 标准2通过：MinIO Bucket 存在（bucket: flink-checkpoints）",
    "✅ 标准3通过：MinIO 中存在 Checkpoint 文件",
    "✅ 标准4通过：Checkpoint 包含 _metadata 元数据文件",
    "✅ 标准5通过：Checkpoint 文件数量 = 8，总大小 = 3.0 MB",
    "✅ 标准6通过：MinIO 可替代 SeaweedFS 作为 Checkpoint 存储"
  ],
  "failedCriteria": [],
  "seaweedFs": {
    "path": "/mnt/seaweedfs/flink/checkpoints",
    "accessible": true,
    "checkpointExists": true,
    "checkpointFileCount": 12,
    "totalCheckpointSize": 5242880
  },
  "minio": {
    "endpoint": "http://localhost:9000",
    "accessible": true,
    "bucketExists": true,
    "checkpointExists": true,
    "hasMetadata": true,
    "checkpointFileCount": 8,
    "totalCheckpointSize": 3145728
  }
}
```

#### 7.5.4 迁移验证完整流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                     迁移验证完整流程                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  步骤1：迁移前 - 验证 SeaweedFS 基准                              │
│  ┌──────────────────────────────────────────┐                   │
│  │ curl .../migration/verify-seaweedfs      │                   │
│  │   → accessible = true?                   │                   │
│  │   → checkpointExists = true?             │                   │
│  │   → 记录 checkpointFileCount 基准值       │                   │
│  └──────────────────┬───────────────────────┘                   │
│                     │ 通过                                       │
│                     ▼                                           │
│  步骤2：迁移配置 - 修改代码和配置                                  │
│  ┌──────────────────────────────────────────┐                   │
│  │ 1. 添加 flink-s3-fs-hadoop 依赖          │                   │
│  │ 2. 配置 s3.endpoint / access-key 等      │                   │
│  │ 3. Checkpoint 路径 file:// → s3://       │                   │
│  │ 4. 创建 MinIO Bucket                     │                   │
│  └──────────────────┬───────────────────────┘                   │
│                     │ 完成                                       │
│                     ▼                                           │
│  步骤3：迁移后 - 验证 MinIO                                      │
│  ┌──────────────────────────────────────────┐                   │
│  │ curl .../migration/verify-minio          │                   │
│  │   → accessible = true?                   │                   │
│  │   → bucketExists = true?                 │                   │
│  │   → checkpointExists = true?             │                   │
│  │   → hasMetadata = true?                  │                   │
│  └──────────────────┬───────────────────────┘                   │
│                     │ 通过                                       │
│                     ▼                                           │
│  步骤4：对比验证 - 综合判定                                       │
│  ┌──────────────────────────────────────────┐                   │
│  │ curl .../migration/compare               │                   │
│  │   → 核心3项标准是否全部通过?               │                   │
│  │   → 给出 migrationSuccessful 判定         │                   │
│  └──────────────────┬───────────────────────┘                   │
│                     │                                           │
│          ┌──────────┴──────────┐                                │
│          │                     │                                │
│     成功 ▼                失败 ▼                                │
│  ┌─────────────┐     ┌──────────────┐                          │
│  │ 🎉 迁移完成  │     │ ⚠️ 排查问题   │                          │
│  │ 后续：       │     │ 参考 7.5.5   │                          │
│  │ 1.恢复测试   │     │ 常见失败原因  │                          │
│  │ 2.性能监控   │     │              │                          │
│  └─────────────┘     └──────────────┘                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 7.5.5 迁移失败常见原因及排查

| 失败标准 | 可能原因 | 排查方法 |
|----------|----------|----------|
| 标准1失败：MinIO 连接失败 | MinIO 未启动、endpoint 错误 | `curl http://localhost:9000/minio/health/live` |
| 标准2失败：Bucket 不存在 | 未创建 Bucket | `curl -X POST .../migration/init-bucket` |
| 标准3失败：无 Checkpoint | 作业未运行或未触发 Checkpoint | `curl -X POST .../job/submit` 后等待 30s |
| 标准4失败：无 _metadata | Checkpoint 写入不完整 | 检查 Flink 日志是否有异常 |
| 标准5失败：文件数为 0 | 同标准3 | 同上 |
| 标准6失败：MinIO 不可用 | 综合问题 | 依次排查标准1-2 |

#### 7.5.6 迁移后恢复测试

迁移验证通过后，还需验证从 MinIO Checkpoint 恢复作业的能力：

```bash
# 1. 提交作业并等待 Checkpoint
curl -X POST http://localhost:8080/api/flink/job/submit
sleep 30

# 2. 记录当前 Checkpoint 路径
curl http://localhost:8080/api/flink/checkpoint/list

# 3. 停止作业（模拟故障）

# 4. 从 Checkpoint 恢复（使用 -s 参数指定 savepoint 路径）
# Flink 命令行方式：
# flink run -s s3://flink-checkpoints/flink/checkpoints/<job-id>/chk-1 -d your-job.jar
```

**恢复验证标准**：
- ✅ 作业能从 MinIO Checkpoint 成功启动
- ✅ 恢复后状态数据与停止前一致
- ✅ 恢复后新数据能正常处理
- ✅ 恢复后 Checkpoint 继续正常写入

### 7.6 迁移验证 API 速查表

| API | 方法 | 说明 |
|-----|------|------|
| `/api/flink/migration/verify-seaweedfs?path=...` | GET | 验证 SeaweedFS 挂载路径 |
| `/api/flink/migration/verify-seaweedfs-filer?filerUrl=...&checkpointPath=...` | GET | 验证 SeaweedFS Filer（HTTP API 模式）|
| `/api/flink/migration/verify-seaweedfs-config` | GET | 使用配置文件验证 SeaweedFS |
| `/api/flink/migration/verify-minio` | GET | 验证新 MinIO 存储 |
| `/api/flink/migration/compare?seaweedFsPath=...` | GET | 完整迁移对比验证 |
| `/api/flink/migration/init-bucket` | POST | 初始化 MinIO Bucket |
| `/api/flink/job/submit` | POST | 提交 Flink 作业 |
| `/api/flink/checkpoint/exists` | GET | 检查 Checkpoint 是否存在 |
| `/api/flink/checkpoint/list` | GET | 列出 Checkpoint 文件 |
| `/api/flink/config` | GET | 查看当前配置 |

***

## 8. 常见问题排查

### 8.1 S3 Endpoint 配置错误

**症状**：

```
org.apache.flink.fs.s3.common.shaded.com.amazonaws.SdkClientException:
Unable to execute HTTP request: Connect to localhost:9000 failed
```

**原因**：MinIO 未启动或 endpoint 地址错误。

**解决**：

```bash
# 检查 MinIO 是否运行
docker ps | grep minio

# 检查端口是否正确
curl http://localhost:9000/minio/health/live

# 确保 application.yml 中的 endpoint 正确
minio:
  endpoint: http://localhost:9000  # 确认端口号
```

### 8.2 Access Denied / 权限问题

**症状**：

```
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied
```

**原因**：access-key 或 secret-key 不正确。

**解决**：

```yaml
# 确认 MinIO 的访问密钥
minio:
  access-key: minioadmin    # 与 MinIO 启动时的 MINIO_ROOT_USER 一致
  secret-key: minioadmin    # 与 MinIO 启动时的 MINIO_ROOT_PASSWORD 一致
```

### 8.3 Bucket 不存在

**症状**：

```
com.amazonaws.services.s3.model.AmazonS3Exception: The specified bucket does not exist
```

**原因**：MinIO 中未创建对应的 Bucket。

**解决**：

```bash
# 使用 mc 创建 Bucket
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/flink-checkpoints

# 或在代码中自动创建（MinioInitService.ensureBucketExists()）
```

### 8.4 路径风格访问错误

**症状**：

```
com.amazonaws.services.s3.model.AmazonS3Exception: NoSuchBucket
```

但 Bucket 明明存在。

**原因**：MinIO 使用路径风格访问，但 Flink 默认使用虚拟主机风格。

**解决**：

```java
// 必须设置路径风格访问为 true
flinkConfig.setString("s3.path.style.access", "true");
```

### 8.5 Flink S3 插件未加载

**症状**：

```
org.apache.flink.core.fs.UnsupportedFileSystemSchemeException:
Could not find a file system implementation for scheme 's3'
```

**原因**：缺少 `flink-s3-fs-hadoop` 依赖。

**解决**：

```xml
<!-- 在 pom.xml 中添加 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-s3-fs-hadoop</artifactId>
    <version>${flink.version}</version>
</dependency>
```

### 8.6 Checkpoint 超时

**症状**：

```
Checkpoint expired before completing
```

**原因**：Checkpoint 写入 MinIO 的速度太慢，超过了超时时间。

**解决**：

```java
// 增加超时时间
checkpointConfig.setCheckpointTimeout(600000); // 10 分钟

// 或减少状态大小（使用增量 Checkpoint）
EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true); // true = 增量
```

### 8.7 Testcontainers 无法启动 MinIO

**症状**：

```
org.testcontainers.containers.ContainerLaunchException: Container startup failed
```

**原因**：Docker 未运行或端口冲突。

**解决**：

```bash
# 确保 Docker 正在运行
docker info

# 检查是否有端口冲突
netstat -an | grep 9000
```

### 8.8 Java 模块访问错误

**症状**：

```
InaccessibleObjectException: Unable to make field accessible
```

**原因**：Java 21 的模块系统限制。

**解决**：在 Maven Surefire 插件中添加 JVM 参数（已在 pom.xml 中配置）：

```xml
<argLine>
    --add-opens=java.base/java.nio=ALL-UNNAMED
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.util=ALL-UNNAMED
</argLine>
```

***

## 附录

### A. Flink S3 配置完整参考

| 配置项                             | 说明            | 默认值       |
| ------------------------------- | ------------- | --------- |
| `s3.endpoint`                   | S3 服务地址       | AWS 地址    |
| `s3.access-key`                 | 访问密钥 ID       | 无         |
| `s3.secret-key`                 | 密钥            | 无         |
| `s3.path.style.access`          | 路径风格访问        | false     |
| `s3.connection.maximum`         | 最大连接数         | 500       |
| `s3.connection.timeout`         | 连接超时（毫秒）      | 5000      |
| `s3.socket.timeout`             | Socket 超时（毫秒） | 50000     |
| `s3.upload.part.size`           | 分片上传大小（字节）    | 104857600 |
| `s3.multipart.upload.threshold` | 分片上传阈值        | 20971520  |

### B. MinIO Docker Compose 示例

```yaml
version: '3'
services:
  minio:
    image: minio/minio:RELEASE.2024-01-16T16-07-38Z
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data

volumes:
  minio-data:
```

### C. SeaweedFS Docker Compose 示例（迁移前参考）

```yaml
version: '3'
services:
  seaweedfs-master:
    image: chrislusf/seaweedfs:3.45
    command: "master -ip=seaweedfs-master"
    ports:
      - "9333:9333"
      - "19333:19333"

  seaweedfs-volume:
    image: chrislusf/seaweedfs:3.45
    command: "volume -mserver=seaweedfs-master:9333 -port=8080"
    ports:
      - "8080:8080"
    depends_on:
      - seaweedfs-master

  seaweedfs-filer:
    image: chrislusf/seaweedfs:3.45
    command: "filer -master=seaweedfs-master:9333"
    ports:
      - "8888:8888"
    depends_on:
      - seaweedfs-master
      - seaweedfs-volume
```

### D. 关键术语表

| 术语            | 英文                     | 解释                                 |
| ------------- | ---------------------- | ---------------------------------- |
| Checkpoint    | Checkpoint             | Flink 定期保存的作业状态快照                  |
| 状态后端          | State Backend          | Flink 管理状态的组件                      |
| Savepoint     | Savepoint              | 手动触发的状态快照，用于版本升级等                  |
| S3 协议         | S3 Protocol            | Amazon S3 定义的存储访问协议                |
| Bucket        | Bucket                 | S3 中的存储桶，类似文件夹                     |
| Object        | Object                 | S3 中的存储对象，类似文件                     |
| 路径风格          | Path Style             | `http://endpoint/bucket/object` 格式 |
| 虚拟主机风格        | Virtual Host Style     | `http://bucket.endpoint/object` 格式 |
| 增量 Checkpoint | Incremental Checkpoint | 只保存变化部分，减少写入量                      |

