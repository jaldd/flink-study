package com.example.flinkminio.config;

import io.minio.BucketExistsArgs;
import io.minio.MinioClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class MinioHealthIndicator implements HealthIndicator {

    private final MinioClient minioClient;
    private final MinioProperties minioProperties;

    public MinioHealthIndicator(MinioClient minioClient, MinioProperties minioProperties) {
        this.minioClient = minioClient;
        this.minioProperties = minioProperties;
    }

    @Override
    public Health health() {
        try {
            boolean bucketExists = minioClient.bucketExists(
                BucketExistsArgs.builder()
                    .bucket(minioProperties.getBucket())
                    .build()
            );

            if (bucketExists) {
                return Health.up()
                    .withDetail("endpoint", minioProperties.getEndpoint())
                    .withDetail("bucket", minioProperties.getBucket())
                    .build();
            } else {
                return Health.up()
                    .withDetail("endpoint", minioProperties.getEndpoint())
                    .withDetail("bucket", minioProperties.getBucket())
                    .withDetail("bucketExists", false)
                    .withDetail("message", "Bucket 不存在，提交作业时会自动创建")
                    .build();
            }
        } catch (Exception e) {
            return Health.down()
                .withDetail("endpoint", minioProperties.getEndpoint())
                .withDetail("bucket", minioProperties.getBucket())
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}
