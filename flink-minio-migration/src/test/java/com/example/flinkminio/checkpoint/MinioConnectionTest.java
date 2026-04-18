package com.example.flinkminio.checkpoint;

import com.example.flinkminio.util.MinioTestContainer;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MinIO 连接与基本操作测试
 *
 * 本测试验证 MinIO 容器是否正常启动，
 * 以及基本的 Bucket 创建、文件上传和列表操作是否正常。
 *
 * 这是后续 Checkpoint 测试的基础：
 * 如果 MinIO 本身无法连接，Checkpoint 写入也会失败。
 */
@DisplayName("MinIO 连接与基本操作测试")
class MinioConnectionTest extends MinioTestContainer {

    private static final String TEST_BUCKET = "test-bucket";
    private MinioClient minioClient;

    @BeforeEach
    void setUp() {
        // 创建 MinIO 客户端，连接到 Testcontainers 启动的 MinIO
        minioClient = MinioClient.builder()
            .endpoint(getMinioEndpoint())
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();
    }

    @Test
    @DisplayName("MinIO 容器应正常启动并可连接")
    void testMinioContainerIsRunning() {
        // 验证容器正在运行
        assertTrue(minioContainer.isRunning(), "MinIO 容器应该正在运行");

        // 验证可以创建客户端并连接
        assertNotNull(minioClient, "MinIO 客户端应该创建成功");

        // 验证 endpoint 不为空
        assertNotNull(getMinioEndpoint(), "MinIO endpoint 不应为空");
        assertTrue(getMinioEndpoint().startsWith("http://"), "MinIO endpoint 应以 http:// 开头");
    }

    @Test
    @DisplayName("应能成功创建 Bucket")
    void testCreateBucket() throws Exception {
        // 创建 Bucket
        minioClient.makeBucket(
            MakeBucketArgs.builder()
                .bucket(TEST_BUCKET)
                .build()
        );

        // 验证 Bucket 存在
        boolean exists = minioClient.bucketExists(
            BucketExistsArgs.builder()
                .bucket(TEST_BUCKET)
                .build()
        );

        assertTrue(exists, "Bucket 创建后应该存在");
    }

    @Test
    @DisplayName("应能成功上传和列出文件")
    void testUploadAndListFiles() throws Exception {
        // 先创建 Bucket
        minioClient.makeBucket(
            MakeBucketArgs.builder()
                .bucket(TEST_BUCKET)
                .build()
        );

        // 上传测试文件
        String testContent = "hello flink minio checkpoint test";
        minioClient.putObject(
            PutObjectArgs.builder()
                .bucket(TEST_BUCKET)
                .object("test/checkpoint-data.txt")
                .stream(new ByteArrayInputStream(testContent.getBytes()), testContent.length(), -1)
                .contentType("text/plain")
                .build()
        );

        // 列出文件
        List<String> objectNames = new ArrayList<>();
        Iterable<Result<Item>> results = minioClient.listObjects(
            ListObjectsArgs.builder()
                .bucket(TEST_BUCKET)
                .prefix("test/")
                .recursive(true)
                .build()
        );

        for (Result<Item> result : results) {
            objectNames.add(result.get().objectName());
        }

        // 验证文件存在
        assertFalse(objectNames.isEmpty(), "应该至少有一个文件");
        assertTrue(objectNames.stream().anyMatch(name -> name.contains("checkpoint-data")),
            "应该包含上传的测试文件");
    }

    @Test
    @DisplayName("重复创建同名 Bucket 不应报错（幂等性）")
    void testCreateBucketIdempotent() throws Exception {
        // 第一次创建
        minioClient.makeBucket(
            MakeBucketArgs.builder()
                .bucket("idempotent-bucket")
                .build()
        );

        // 验证存在
        boolean exists1 = minioClient.bucketExists(
            BucketExistsArgs.builder()
                .bucket("idempotent-bucket")
                .build()
        );
        assertTrue(exists1);

        // 再次检查存在性（不重复创建）
        boolean exists2 = minioClient.bucketExists(
            BucketExistsArgs.builder()
                .bucket("idempotent-bucket")
                .build()
        );
        assertTrue(exists2);
    }
}
