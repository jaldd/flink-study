package com.example.flinkminio.service;

import com.example.flinkminio.config.FlinkConfig;
import com.example.flinkminio.config.MinioProperties;
import com.example.flinkminio.job.WordCountJob;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CheckpointService {

    private static final Logger log = LoggerFactory.getLogger(CheckpointService.class);

    private final FlinkConfig flinkConfig;
    private final MinioProperties minioProperties;
    private final MinioClient minioClient;
    private final MinioInitService minioInitService;

    private final ConcurrentHashMap<String, StreamExecutionEnvironment> runningJobs = new ConcurrentHashMap<>();

    public CheckpointService(FlinkConfig flinkConfig, MinioProperties minioProperties,
                             MinioClient minioClient, MinioInitService minioInitService) {
        this.flinkConfig = flinkConfig;
        this.minioProperties = minioProperties;
        this.minioClient = minioClient;
        this.minioInitService = minioInitService;
    }

    public String submitWordCountJob(String inputFilePath) {
        try {
            minioInitService.ensureBucketExists();

            StreamExecutionEnvironment env = flinkConfig.createExecutionEnvironment();
            WordCountJob.buildPipeline(env, inputFilePath);

            String jobId = env.executeAsync(WordCountJob.JOB_NAME).getJobID().toString();
            runningJobs.put(jobId, env);

            log.info("Flink 作业已提交, jobId: {}, jobName: {}", jobId, WordCountJob.JOB_NAME);
            return jobId;
        } catch (Exception e) {
            log.error("提交 Flink 作业失败", e);
            throw new RuntimeException("提交 Flink 作业失败: " + e.getMessage(), e);
        }
    }

    public void removeJob(String jobId) {
        StreamExecutionEnvironment removed = runningJobs.remove(jobId);
        if (removed != null) {
            log.info("已从运行列表移除作业, jobId: {}", jobId);
        }
    }

    public Set<String> getRunningJobIds() {
        return runningJobs.keySet();
    }

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

    public boolean checkpointExists() {
        return !listCheckpointFiles().isEmpty();
    }
}
