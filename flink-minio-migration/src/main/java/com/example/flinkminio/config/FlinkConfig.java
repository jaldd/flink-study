package com.example.flinkminio.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Flink 执行环境配置类
 *
 * 本类是整个迁移的核心：将 Flink Checkpoint 存储配置为 MinIO（通过 S3 协议）。
 *
 * 【SeaweedFS 迁移对比 - 这是最关键的配置差异】
 *
 * === 使用 SeaweedFS 时的配置 ===
 * SeaweedFS 通常有两种使用方式：
 *
 * 方式1：SeaweedFS 作为本地文件系统挂载
 *   env.checkpointConfig().setCheckpointStorage("file:///mnt/seaweedfs/flink/checkpoints");
 *   - 依赖操作系统将 SeaweedFS 挂载为本地目录
 *   - 简单但不够可靠，挂载点可能失效
 *
 * 方式2：SeaweedFS 的 Hadoop 兼容接口
 *   env.checkpointConfig().setCheckpointStorage("hdfs://seaweedfs-namenode:9200/flink/checkpoints");
 *   - 需要 SeaweedFS 的 Hadoop 兼容层
 *   - 配置复杂，依赖 Hadoop 客户端
 *
 * === 迁移到 MinIO 后的配置 ===
 *   env.checkpointConfig().setCheckpointStorage("s3://flink-checkpoints/flink/checkpoints");
 *   - 使用标准 S3 协议，MinIO 完全兼容
 *   - 只需配置 endpoint 指向 MinIO 地址
 *   - 无需额外挂载或 Hadoop 兼容层
 *
 * 迁移步骤：
 * 1. 添加 flink-s3-fs-hadoop 依赖
 * 2. 配置 S3 endpoint 为 MinIO 地址
 * 3. 配置 S3 访问密钥
 * 4. 将 Checkpoint 路径从 file:// 或 hdfs:// 改为 s3://
 * 5. 使用 RocksDB 状态后端（推荐用于大状态场景）
 */
@Configuration
public class FlinkConfig {

    private static final Logger log = LoggerFactory.getLogger(FlinkConfig.class);

    @Autowired
    private MinioProperties minioProperties;

    /**
     * 创建并配置 Flink StreamExecutionEnvironment
     *
     * 这是 Flink 作业的执行环境，所有配置都在这里完成。
     * 关键配置包括：
     * - Checkpoint 间隔（多久做一次快照）
     * - Checkpoint 存储位置（MinIO 的 S3 路径）
     * - 状态后端（RocksDB，用于管理大状态）
     * - S3 连接参数（endpoint、密钥等）
     *
     * @return 配置好的 StreamExecutionEnvironment
     */
    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        // 创建 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // =====================================================
        // 第一步：配置状态后端
        // =====================================================
        // RocksDB 状态后端：将状态存储在 RocksDB 中（本地磁盘 + 堆外内存）
        // 适合大状态场景，支持增量 Checkpoint
        //
        // 【对比】HashMapStateBackend（默认）：
        // - 状态存储在 JVM 堆内存中
        // - 适合小状态场景
        // - 配置方式：env.setStateBackend(new HashMapStateBackend())
        //
        // 推荐使用 RocksDB，因为生产环境状态通常较大
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDBStateBackend);

        // =====================================================
        // 第二步：配置 S3 文件系统（连接 MinIO 的关键）
        // =====================================================
        // 这些配置告诉 Flink 的 S3 插件如何连接 MinIO
        // MinIO 兼容 S3 协议，所以使用 S3 配置即可
        //
        // 【SeaweedFS 对比】
        // SeaweedFS 不需要这些 S3 配置（除非使用 S3 Gateway 模式）
        // 迁移时必须添加以下配置

        // S3 endpoint：指向 MinIO 地址，而不是 AWS
        // 这是迁移的关键配置！告诉 Flink 不要连 AWS，而是连 MinIO
        env.getConfig().setGlobalJobParameters(
            new org.apache.flink.configuration.Configuration()
        );

        org.apache.flink.configuration.Configuration flinkConfig = new org.apache.flink.configuration.Configuration();

        // S3 Endpoint 配置：指向 MinIO 服务地址
        flinkConfig.setString("s3.endpoint", minioProperties.getEndpoint());

        // S3 访问密钥：MinIO 的 access key 和 secret key
        flinkConfig.setString("s3.access-key", minioProperties.getAccessKey());
        flinkConfig.setString("s3.secret-key", minioProperties.getSecretKey());

        // 禁用路径风格访问（MinIO 需要路径风格）
        // MinIO 默认使用路径风格：http://minio:9000/bucket/object
        // AWS 使用虚拟主机风格：http://bucket.s3.amazonaws.com/object
        flinkConfig.setString("s3.path.style.access", "true");

        // 将配置注册到全局
        env.getConfig().setGlobalJobParameters(flinkConfig);

        // =====================================================
        // 第三步：配置 Checkpoint
        // =====================================================
        // Checkpoint 是 Flink 容错的核心机制
        // 它定期将作业的状态快照保存到持久化存储（这里是 MinIO）

        // 开启 Checkpoint，间隔 30 秒（即每 30 秒做一次快照）
        env.enableCheckpointing(30000);

        // 获取 Checkpoint 配置对象
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // Checkpoint 模式：EXACTLY_ONCE（精确一次语义）
        // 这意味着每条数据只会被处理一次，不会丢失也不会重复
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Checkpoint 超时时间：10 分钟
        // 如果 Checkpoint 在 10 分钟内未完成，则视为失败
        checkpointConfig.setCheckpointTimeout(600000);

        // 最小间隔：500 毫秒
        // 两个 Checkpoint 之间至少间隔 500ms，避免过于频繁
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // 最大并发 Checkpoint 数：1
        // 同一时间只允许一个 Checkpoint 在进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // =====================================================
        // 第四步：配置 Checkpoint 存储（迁移的核心修改点）
        // =====================================================
        // 【SeaweedFS 配置】（迁移前）：
        // checkpointConfig.setCheckpointStorage("file:///mnt/seaweedfs/flink/checkpoints");
        // 或
        // checkpointConfig.setCheckpointStorage("hdfs://seaweedfs:9200/flink/checkpoints");
        //
        // 【MinIO 配置】（迁移后）：
        // 使用 S3 协议路径，指向 MinIO 的 bucket
        String checkpointPath = minioProperties.getFullCheckpointPath();
        log.info("设置 Checkpoint 存储路径: {}", checkpointPath);
        checkpointConfig.setCheckpointStorage(checkpointPath);

        // 当作业取消时，保留 Checkpoint 数据
        // 这样即使作业被取消，也可以从 Checkpoint 恢复
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 容忍的 Checkpoint 失败次数：3
        // 如果连续 3 次 Checkpoint 失败，则作业失败
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // =====================================================
        // 第五步：配置重启策略
        // =====================================================
        // 固定延迟重启策略：最多重启 3 次，每次间隔 10 秒
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS))
        );

        log.info("Flink 执行环境配置完成，Checkpoint 存储路径: {}", checkpointPath);
        return env;
    }
}
