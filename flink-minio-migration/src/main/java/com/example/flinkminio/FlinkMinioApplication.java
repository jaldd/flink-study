package com.example.flinkminio;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot 启动类
 *
 * 本项目演示如何将 Flink Checkpoint 存储从 SeaweedFS 迁移到 MinIO。
 *
 * 核心概念：
 * - Flink Checkpoint：Flink 的容错机制，定期将作业状态保存到持久化存储
 * - SeaweedFS：一个分布式文件系统，之前用作 Checkpoint 存储
 * - MinIO：兼容 S3 协议的对象存储，迁移目标
 *
 * 迁移的关键点：
 * - SeaweedFS 通常使用 file:// 或 hdfs:// 协议路径
 * - MinIO 使用 s3:// 协议路径，需要 Flink S3 文件系统插件
 * - 需要配置 S3 endpoint 指向 MinIO 地址（而非 AWS）
 * - 需要配置访问密钥（access key / secret key）
 */
@SpringBootApplication
public class FlinkMinioApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkMinioApplication.class, args);
    }
}
