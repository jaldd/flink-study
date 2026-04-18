package com.example.flinkminio.checkpoint;

import com.example.flinkminio.util.MinioTestContainer;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checkpoint 集成测试
 *
 * 使用 Flink MiniCluster + Testcontainers MinIO 进行集成测试。
 * MiniCluster 是 Flink 提供的轻量级测试集群，可以在单 JVM 中运行 Flink 作业。
 *
 * 测试流程：
 * 1. Testcontainers 启动 MinIO 容器
 * 2. 手动启动 Flink MiniCluster
 * 3. 配置 Flink Checkpoint 存储到 MinIO
 * 4. 运行作业并触发 Checkpoint
 * 5. 验证 MinIO 中存在 Checkpoint 文件
 *
 * 【SeaweedFS 对比】
 * 如果使用 SeaweedFS，此测试需要：
 * 1. 启动 SeaweedFS 容器（Filer + Volume Server）
 * 2. 配置 Checkpoint 路径为 file:// 或 hdfs://
 * 3. 验证方式不同（通过 Filer API 或挂载目录）
 *
 * 迁移到 MinIO 后：
 * 1. 只需一个 MinIO 容器
 * 2. 使用 S3 协议配置
 * 3. 通过 S3 API 验证
 */
@DisplayName("Checkpoint 集成测试 - MiniCluster + MinIO")
class CheckpointIntegrationTest extends MinioTestContainer {

    private static final String CHECKPOINT_BUCKET = "flink-checkpoints";

    /**
     * Flink MiniCluster 实例
     *
     * MiniCluster 是 Flink 的嵌入式测试集群：
     * - 在单 JVM 中模拟完整的 Flink 集群
     * - 包含 JobManager 和 TaskManager
     * - 支持 Checkpoint、状态后端等所有功能
     * - 需要手动启动和关闭
     */
    private MiniCluster miniCluster;

    private MinioClient minioClient;

    @BeforeEach
    void setUp() throws Exception {
        // 创建并启动 MiniCluster
        Configuration config = new Configuration();
        config.setInteger(CoreOptions.DEFAULT_PARALLELISM, 1);
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(16));

        MiniClusterConfiguration clusterConfig = new MiniClusterConfiguration.Builder()
            .setNumTaskManagers(1)
            .setNumSlotsPerTaskManager(4)
            .setConfiguration(config)
            .build();

        miniCluster = new MiniCluster(clusterConfig);
        miniCluster.start();

        // 创建 MinIO 客户端
        minioClient = MinioClient.builder()
            .endpoint(getMinioEndpoint())
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();

        // 确保 Checkpoint Bucket 存在
        boolean bucketExists = minioClient.bucketExists(
            BucketExistsArgs.builder()
                .bucket(CHECKPOINT_BUCKET)
                .build()
        );

        if (!bucketExists) {
            minioClient.makeBucket(
                MakeBucketArgs.builder()
                    .bucket(CHECKPOINT_BUCKET)
                    .build()
            );
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    /**
     * 创建配置了 MinIO Checkpoint 的 Flink 执行环境
     *
     * 这是迁移的核心：将 Checkpoint 存储配置为 MinIO（S3 协议）
     *
     * 【SeaweedFS 迁移对比】
     * 迁移前（SeaweedFS）：
     *   checkpointConfig.setCheckpointStorage("file:///mnt/seaweedfs/flink/checkpoints");
     *
     * 迁移后（MinIO）：
     *   checkpointConfig.setCheckpointStorage("s3://flink-checkpoints/flink/checkpoints");
     *   + 配置 S3 endpoint 为 MinIO 地址
     *   + 配置 S3 访问密钥
     */
    private StreamExecutionEnvironment createEnvironmentWithMinioCheckpoint() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 RocksDB 状态后端（支持增量 Checkpoint）
        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(stateBackend);

        // 配置 S3 文件系统连接 MinIO
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("s3.endpoint", getMinioEndpoint());
        flinkConfig.setString("s3.access-key", ACCESS_KEY);
        flinkConfig.setString("s3.secret-key", SECRET_KEY);
        flinkConfig.setString("s3.path.style.access", "true");
        env.getConfig().setGlobalJobParameters(flinkConfig);

        // 开启 Checkpoint
        env.enableCheckpointing(1000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 【关键迁移点】设置 Checkpoint 存储路径为 MinIO S3 路径
        String checkpointPath = "s3://" + CHECKPOINT_BUCKET + "/flink/checkpoints";
        checkpointConfig.setCheckpointStorage(checkpointPath);

        // 保留 Checkpoint 数据（即使作业取消）
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 配置重启策略
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.SECONDS))
        );

        return env;
    }

    @Test
    @DisplayName("Flink 作业应能成功写入 Checkpoint 到 MinIO")
    void testCheckpointWritesToMinio() throws Exception {
        // 创建配置了 MinIO Checkpoint 的执行环境
        StreamExecutionEnvironment env = createEnvironmentWithMinioCheckpoint();

        // 创建简单的流处理作业
        env.fromElements("hello minio", "flink checkpoint", "s3 storage")
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String line, Collector<String> out) {
                    for (String word : line.split("\\s+")) {
                        out.collect(word);
                    }
                }
            })
            .keyBy(word -> word)
            .sum(1)
            .print();

        // 执行作业
        env.execute("test-checkpoint-minio");

        // 等待 Checkpoint 写入
        Thread.sleep(5000);

        // 验证 MinIO 中存在 Checkpoint 文件
        List<String> checkpointFiles = listCheckpointFiles();
        assertFalse(checkpointFiles.isEmpty(),
            "MinIO 中应该存在 Checkpoint 文件，但未找到任何文件。" +
            "请检查 S3 endpoint 配置是否正确: " + getMinioEndpoint());
    }

    @Test
    @DisplayName("Checkpoint 文件应包含元数据")
    void testCheckpointContainsMetadata() throws Exception {
        StreamExecutionEnvironment env = createEnvironmentWithMinioCheckpoint();

        env.fromElements("test metadata")
            .print();

        env.execute("test-checkpoint-metadata");

        Thread.sleep(5000);

        List<String> files = listCheckpointFiles();

        // Checkpoint 应包含元数据文件
        // Flink Checkpoint 目录结构：
        //   chk-1/
        //     _metadata          <- Checkpoint 元数据
        //     owned/             <- 状态文件
        //     <operator-id>/     <- 算子状态
        boolean hasMetadata = files.stream()
            .anyMatch(name -> name.contains("_metadata") || name.contains("chk-"));

        assertTrue(hasMetadata,
            "Checkpoint 文件应包含元数据文件（_metadata），实际文件: " + files);
    }

    /**
     * 列出 MinIO 中的 Checkpoint 文件
     */
    private List<String> listCheckpointFiles() throws Exception {
        List<String> files = new ArrayList<>();
        Iterable<Result<Item>> results = minioClient.listObjects(
            ListObjectsArgs.builder()
                .bucket(CHECKPOINT_BUCKET)
                .prefix("flink/checkpoints")
                .recursive(true)
                .build()
        );

        for (Result<Item> result : results) {
            files.add(result.get().objectName());
        }
        return files;
    }
}
