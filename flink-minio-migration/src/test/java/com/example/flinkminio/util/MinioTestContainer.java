package com.example.flinkminio.util;

import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * MinIO 测试容器工具类
 *
 * 使用 Testcontainers 自动启动 MinIO Docker 容器进行测试。
 * 这样无需手动安装和启动 MinIO，测试可以自动运行。
 *
 * 【原理】
 * Testcontainers 是一个 Java 测试库，它会在测试运行时：
 * 1. 自动拉取 MinIO Docker 镜像
 * 2. 启动 MinIO 容器
 * 3. 分配随机端口（避免端口冲突）
 * 4. 测试结束后自动销毁容器
 *
 * 【SeaweedFS 对比】
 * 如果要测试 SeaweedFS，可以使用类似的 Testcontainers 方式：
 * - 使用 SeaweedFS 的 Docker 镜像：chrislusf/seaweedfs
 * - 启动 Filer + Volume Server
 * - 配置路径为 http://localhost:<port>/path
 *
 * 迁移后，只需将容器镜像从 SeaweedFS 改为 MinIO，
 * 路径协议从 http:// 改为 s3://
 */
@Testcontainers
public abstract class MinioTestContainer {

    /**
     * MinIO Docker 镜像版本
     */
    private static final String MINIO_IMAGE = "minio/minio:RELEASE.2024-01-16T16-07-38Z";

    /**
     * MinIO 默认访问密钥
     */
    protected static final String ACCESS_KEY = "minioadmin";
    protected static final String SECRET_KEY = "minioadmin";

    /**
     * MinIO 测试容器
     *
     * @Container 注解让 Testcontainers 自动管理容器生命周期
     * withUserName / withPassword 设置 MinIO 的访问密钥
     */
    @Container
    protected static final MinIOContainer minioContainer = new MinIOContainer(
        DockerImageName.parse(MINIO_IMAGE)
    )
        .withUserName(ACCESS_KEY)
        .withPassword(SECRET_KEY);

    /**
     * 获取 MinIO 的 S3 endpoint 地址
     *
     * 容器启动后，Testcontainers 会分配一个随机端口，
     * 此方法返回完整的 HTTP 地址
     *
     * @return 例如 http://localhost:54321
     */
    protected String getMinioEndpoint() {
        return String.format("http://%s:%d",
            minioContainer.getHost(),
            minioContainer.getMappedPort(9000)
        );
    }

    /**
     * 获取 MinIO Console 地址（Web UI）
     *
     * @return 例如 http://localhost:54322
     */
    protected String getMinioConsoleEndpoint() {
        return String.format("http://%s:%d",
            minioContainer.getHost(),
            minioContainer.getMappedPort(9001)
        );
    }
}
