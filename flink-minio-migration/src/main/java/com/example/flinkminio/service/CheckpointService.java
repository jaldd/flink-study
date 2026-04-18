package com.example.flinkminio.service;

import com.example.flinkminio.config.MinioProperties;
import com.example.flinkminio.job.WordCountJob;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Checkpoint 管理服务
 *
 * 提供以下功能：
 * 1. 提交 Flink 作业
 * 2. 查询 MinIO 中的 Checkpoint 文件
 * 3. 验证 Checkpoint 是否成功写入
 *
 * 【SeaweedFS 对比】
 * 在 SeaweedFS 中查看 Checkpoint 文件：
 *   - 挂载模式：直接 ls /mnt/seaweedfs/flink/checkpoints
 *   - Hadoop 模式：hdfs dfs -ls hdfs://seaweedfs:9200/flink/checkpoints
 *
 * 在 MinIO 中查看 Checkpoint 文件：
 *   - S3 API：使用 MinIO Client 或 AWS CLI
 *   - Web UI：访问 MinIO Console（默认 http://localhost:9001）
 *   - 本服务：通过 REST API 查询
 */
@Service
public class CheckpointService {

    private static final Logger log = LoggerFactory.getLogger(CheckpointService.class);

    @Autowired
    private StreamExecutionEnvironment streamExecutionEnvironment;

    @Autowired
    private MinioProperties minioProperties;

    @Autowired
    private MinioClient minioClient;

    @Autowired
    private MinioInitService minioInitService;

    /**
     * 提交 WordCount 作业
     *
     * 在提交前会确保 MinIO Bucket 存在。
     * 作业将在后台异步执行。
     *
     * @param inputFilePath 输入文件路径（可为 null，使用内置数据源）
     * @return 作业名称
     */
    public String submitWordCountJob(String inputFilePath) {
        try {
            // 确保 MinIO Bucket 存在
            minioInitService.ensureBucketExists();

            // 在新线程中执行 Flink 作业（因为 execute 是阻塞的）
            String jobName = WordCountJob.JOB_NAME;
            Thread jobThread = new Thread(() -> {
                try {
                    WordCountJob.execute(streamExecutionEnvironment, inputFilePath);
                } catch (Exception e) {
                    log.error("Flink 作业执行失败", e);
                }
            }, "flink-job-thread");

            jobThread.setDaemon(true);
            jobThread.start();

            log.info("Flink 作业已提交: {}", jobName);
            return jobName;
        } catch (Exception e) {
            log.error("提交 Flink 作业失败", e);
            throw new RuntimeException("提交 Flink 作业失败: " + e.getMessage(), e);
        }
    }

    /**
     * 列出 MinIO 中的 Checkpoint 文件
     *
     * 通过 MinIO Client 遍历 Bucket 中的 Checkpoint 目录，
     * 返回所有文件路径。
     *
     * @return Checkpoint 文件路径列表
     */
    public List<String> listCheckpointFiles() {
        List<String> files = new ArrayList<>();
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket(minioProperties.getBucket())
                    .prefix(minioProperties.getCheckpointPath())
                    .recursive(true)
                    .build()
            );

            for (Result<Item> result : results) {
                Item item = result.get();
                files.add(item.objectName());
                log.debug("发现 Checkpoint 文件: {} (大小: {} bytes)", item.objectName(), item.size());
            }
        } catch (Exception e) {
            log.error("列出 Checkpoint 文件失败", e);
            throw new RuntimeException("列出 Checkpoint 文件失败: " + e.getMessage(), e);
        }
        return files;
    }

    /**
     * 检查 Checkpoint 是否存在
     *
     * @return 如果存在至少一个 Checkpoint 文件则返回 true
     */
    public boolean checkpointExists() {
        return !listCheckpointFiles().isEmpty();
    }
}
