package com.example.flinkminio.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * SeaweedFS 连接配置属性
 *
 * SeaweedFS 有三种访问模式：
 *
 * 1. 挂载模式（Mount）
 *    - SeaweedFS Filer 挂载为本地目录
 *    - 路径格式：file:///mnt/seaweedfs/flink/checkpoints
 *    - 验证方式：直接检查本地文件系统
 *
 * 2. Filer HTTP API 模式（本项目支持）
 *    - 通过 HTTP API 访问 Filer
 *    - 配置：seaweedfs.filer.url = http://filer:8888
 *    - 验证方式：HTTP GET 请求
 *
 * 3. S3 Gateway 模式
 *    - SeaweedFS 启动 S3 兼容接口
 *    - 路径格式：s3://bucket/path
 *    - 验证方式：与 MinIO 相同
 *
 * 本配置主要用于 Filer HTTP API 模式的验证。
 */
@Component
@ConfigurationProperties(prefix = "seaweedfs")
public class SeaweedFsProperties {

    /**
     * SeaweedFS Filer HTTP API 地址
     * 示例：http://seaweedfs-dev-filer.geip-xa-dev-gdmp:8888
     */
    private String filerUrl;

    /**
     * Checkpoint 在 SeaweedFS 中的存储路径
     * 示例：/flink/checkpoints
     */
    private String checkpointPath = "/flink/checkpoints";

    /**
     * 是否启用 SeaweedFS 验证
     * 如果没有 SeaweedFS 环境，设为 false 跳过验证
     */
    private boolean enabled = false;

    /**
     * 连接超时时间（毫秒）
     */
    private int connectTimeout = 5000;

    /**
     * 读取超时时间（毫秒）
     */
    private int readTimeout = 10000;

    public String getFilerUrl() {
        return filerUrl;
    }

    public void setFilerUrl(String filerUrl) {
        this.filerUrl = filerUrl;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * 获取完整的 Filer API URL
     */
    public String getFullCheckpointUrl() {
        if (filerUrl == null || filerUrl.isEmpty()) {
            return null;
        }
        String baseUrl = filerUrl.endsWith("/") ? filerUrl : filerUrl + "/";
        String path = checkpointPath.startsWith("/") ? checkpointPath.substring(1) : checkpointPath;
        return baseUrl + path;
    }
}
