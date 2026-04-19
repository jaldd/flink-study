package com.example.flinkminio.config;

import io.minio.MinioClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MinIO 客户端配置
 *
 * 创建 MinIO Java SDK 的客户端 Bean。
 * 此客户端用于：
 * 1. 初始化 Bucket
 * 2. 验证 Checkpoint 文件是否成功写入
 * 3. 上传测试数据
 *
 * 注意：Flink 本身不使用此客户端访问 MinIO，
 * Flink 通过 S3 文件系统插件（flink-s3-fs-hadoop）访问 MinIO。
 * 此客户端仅用于辅助验证和管理。
 */
@Configuration
public class MinioClientConfig {

    @Bean
    public MinioClient minioClient(MinioProperties minioProperties) {
        return MinioClient.builder()
            .endpoint(minioProperties.getEndpoint())
            .credentials(minioProperties.getAccessKey(), minioProperties.getSecretKey())
            .build();
    }
}
