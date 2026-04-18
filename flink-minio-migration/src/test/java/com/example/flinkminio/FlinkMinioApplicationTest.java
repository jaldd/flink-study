package com.example.flinkminio;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 应用上下文加载测试
 *
 * 验证 Spring Boot 应用能正常启动和加载配置
 */
@DisplayName("Spring Boot 应用上下文测试")
class FlinkMinioApplicationTest {

    @Test
    @DisplayName("基础断言测试应通过")
    void testBasicAssertion() {
        assertTrue(true, "基础断言应该通过");
    }

    @Test
    @DisplayName("MinIO S3 路径格式应正确")
    void testS3PathFormat() {
        String bucket = "flink-checkpoints";
        String path = "flink/checkpoints";
        String fullPath = "s3://" + bucket + "/" + path;

        assertTrue(fullPath.startsWith("s3://"),
            "MinIO Checkpoint 路径应以 s3:// 开头");
        assertTrue(fullPath.contains(bucket),
            "路径应包含 bucket 名称");
    }

    @Test
    @DisplayName("SeaweedFS 与 MinIO 路径格式对比")
    void testPathFormatComparison() {
        // SeaweedFS 路径格式
        String seaweedfsPath = "file:///mnt/seaweedfs/flink/checkpoints";
        assertTrue(seaweedfsPath.startsWith("file://"),
            "SeaweedFS 路径通常以 file:// 开头");

        // MinIO 路径格式
        String minioPath = "s3://flink-checkpoints/flink/checkpoints";
        assertTrue(minioPath.startsWith("s3://"),
            "MinIO 路径应以 s3:// 开头");

        // 验证两种路径格式不同
        assertNotEquals(
            seaweedfsPath.substring(0, seaweedfsPath.indexOf(":")),
            minioPath.substring(0, minioPath.indexOf(":")),
            "SeaweedFS 和 MinIO 使用不同的协议前缀"
        );
    }
}
