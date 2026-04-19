package com.example.flinkminio.service;

import com.example.flinkminio.config.MinioProperties;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;

/**
 * MinIO 初始化服务
 *
 * 负责在应用启动时确保 MinIO 中的 Bucket 存在，
 * 并提供 MinIO 客户端的 Bean 配置。
 *
 * 【SeaweedFS 对比】
 * SeaweedFS 不需要预先创建 Bucket（如果使用 Filer 模式），
 * 但 MinIO 作为 S3 兼容存储，需要先创建 Bucket 才能写入数据。
 *
 * 迁移时需要：
 * 1. 在 MinIO 中创建对应的 Bucket
 * 2. 配置 Bucket 的访问策略（私有或公开）
 */
@Service
public class MinioInitService {

    private static final Logger log = LoggerFactory.getLogger(MinioInitService.class);

    private final MinioProperties minioProperties;
    private final MinioClient minioClient;

    public MinioInitService(MinioProperties minioProperties, MinioClient minioClient) {
        this.minioProperties = minioProperties;
        this.minioClient = minioClient;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        try {
            ensureBucketExists();
            log.info("MinIO 连接验证通过，Bucket 已就绪");
        } catch (Exception e) {
            log.warn("MinIO 连接验证失败，应用已启动但 MinIO 可能不可用: {}", e.getMessage());
        }
    }

    /**
     * 初始化 MinIO Bucket
     *
     * 检查 Bucket 是否存在，如果不存在则创建。
     * Flink 写入 Checkpoint 时需要 Bucket 已存在。
     */
    public void ensureBucketExists() {
        try {
            String bucket = minioProperties.getBucket();
            boolean exists = minioClient.bucketExists(
                BucketExistsArgs.builder()
                    .bucket(bucket)
                    .build()
            );

            if (!exists) {
                minioClient.makeBucket(
                    MakeBucketArgs.builder()
                        .bucket(bucket)
                        .build()
                );
                log.info("创建 MinIO Bucket 成功: {}", bucket);
            } else {
                log.info("MinIO Bucket 已存在: {}", bucket);
            }
        } catch (Exception e) {
            log.error("初始化 MinIO Bucket 失败", e);
            throw new RuntimeException("初始化 MinIO Bucket 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 上传测试文件到 MinIO
     *
     * 用于测试 MinIO 连接是否正常，以及为 Flink 作业提供输入数据
     *
     * @param objectName 对象名称（路径）
     * @param content    文件内容
     */
    public void uploadTestFile(String objectName, String content) {
        try {
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(minioProperties.getBucket())
                    .object(objectName)
                    .stream(new ByteArrayInputStream(content.getBytes()), content.length(), -1)
                    .contentType("text/plain")
                    .build()
            );
            log.info("上传测试文件成功: {}/{}", minioProperties.getBucket(), objectName);
        } catch (Exception e) {
            log.error("上传测试文件失败", e);
            throw new RuntimeException("上传测试文件失败: " + e.getMessage(), e);
        }
    }
}
