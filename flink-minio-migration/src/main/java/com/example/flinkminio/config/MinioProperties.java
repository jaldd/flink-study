package com.example.flinkminio.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * MinIO 连接配置属性
 *
 * 从 application.yml 中读取 minio 前缀的配置项。
 * 这些配置用于：
 * 1. Flink S3 文件系统插件连接 MinIO
 * 2. MinIO Java 客户端验证 Checkpoint 文件
 *
 * 【SeaweedFS 对比】
 * SeaweedFS 如果作为 S3 兼容模式运行，配置方式类似，
 * 但 endpoint 需要指向 SeaweedFS 的 S3 Gateway 地址。
 * 如果 SeaweedFS 使用 Filer 模式，则路径协议不同：
 * - SeaweedFS Filer: http://seaweedfs-filer:8888/path
 * - MinIO (S3):      s3://bucket/path
 *
 * 迁移时需要修改的关键点：
 * 1. endpoint 从 SeaweedFS 地址改为 MinIO 地址
 * 2. 路径协议从 file:// 或 http:// 改为 s3://
 * 3. 确保 accessKey/secretKey 与 MinIO 一致
 */
@Component
@ConfigurationProperties(prefix = "minio")
public class MinioProperties {

    /**
     * MinIO 服务地址（S3 endpoint）
     * 例如：http://localhost:9000
     */
    private String endpoint = "http://localhost:9000";

    /**
     * 访问密钥（相当于 AWS 的 Access Key ID）
     * MinIO 默认为 minioadmin
     */
    private String accessKey = "minioadmin";

    /**
     * 密钥（相当于 AWS 的 Secret Access Key）
     * MinIO 默认为 minioadmin
     */
    private String secretKey = "minioadmin";

    /**
     * 存储 Checkpoint 的 Bucket 名称
     * Flink 会将 Checkpoint 数据写入 s3://<bucket>/flink/checkpoints
     */
    private String bucket = "flink-checkpoints";

    /**
     * Checkpoint 在 Bucket 中的存储路径前缀
     */
    private String checkpointPath = "flink/checkpoints";

    private boolean s3PathStyleAccess = true;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }

    public boolean isS3PathStyleAccess() {
        return s3PathStyleAccess;
    }

    public void setS3PathStyleAccess(boolean s3PathStyleAccess) {
        this.s3PathStyleAccess = s3PathStyleAccess;
    }

    /**
     * 获取完整的 Checkpoint S3 路径
     * 格式：s3://<bucket>/<checkpointPath>
     * 例如：s3://flink-checkpoints/flink/checkpoints
     */
    public String getFullCheckpointPath() {
        return "s3://" + bucket + "/" + checkpointPath;
    }
}
