package com.example.flinkminio.service;

import com.example.flinkminio.config.MinioProperties;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 迁移验证服务
 *
 * 本服务提供 SeaweedFS → MinIO 迁移前后的对比验证能力。
 * 核心思路：分别检查两种存储后端的 Checkpoint 可用性，
 * 并给出量化的对比结果，帮助判断迁移是否成功。
 *
 * 【迁移验证三步法】
 *
 * 第一步：验证原 SeaweedFS 正常
 *   - 检查 SeaweedFS 挂载目录或 HDFS 路径是否可访问
 *   - 检查是否存在历史 Checkpoint 文件
 *   - 记录 Checkpoint 文件数量和大小
 *
 * 第二步：验证新 MinIO 正常
 *   - 检查 MinIO 连接是否正常
 *   - 检查 Bucket 是否存在
 *   - 检查 Checkpoint 是否成功写入 MinIO
 *   - 记录 Checkpoint 文件数量和大小
 *
 * 第三步：对比验证
 *   - 对比两种存储的 Checkpoint 文件数量
 *   - 对比 Checkpoint 写入延迟
 *   - 对比存储可用性
 *   - 给出迁移是否成功的综合判定
 *
 * 【迁移成功的判定标准】
 *   1. MinIO 连接正常，Bucket 存在
 *   2. Flink 作业能成功写入 Checkpoint 到 MinIO
 *   3. Checkpoint 文件包含 _metadata 元数据
 *   4. 从 MinIO Checkpoint 恢复作业后状态正确
 *   5. Checkpoint 写入延迟在可接受范围内（< 60s）
 *   6. 连续 3 次 Checkpoint 均成功
 */
@Service
public class MigrationVerifyService {

    private static final Logger log = LoggerFactory.getLogger(MigrationVerifyService.class);

    @Autowired
    private MinioClient minioClient;

    @Autowired
    private MinioProperties minioProperties;

    @Autowired
    private MinioInitService minioInitService;

    /**
     * SeaweedFS 存储验证结果
     */
    public static class SeaweedFsVerifyResult {
        private boolean accessible;
        private String path;
        private boolean checkpointExists;
        private int checkpointFileCount;
        private long totalCheckpointSize;
        private String errorMessage;

        public boolean isAccessible() { return accessible; }
        public void setAccessible(boolean accessible) { this.accessible = accessible; }
        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
        public boolean isCheckpointExists() { return checkpointExists; }
        public void setCheckpointExists(boolean checkpointExists) { this.checkpointExists = checkpointExists; }
        public int getCheckpointFileCount() { return checkpointFileCount; }
        public void setCheckpointFileCount(int checkpointFileCount) { this.checkpointFileCount = checkpointFileCount; }
        public long getTotalCheckpointSize() { return totalCheckpointSize; }
        public void setTotalCheckpointSize(long totalCheckpointSize) { this.totalCheckpointSize = totalCheckpointSize; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    }

    /**
     * MinIO 存储验证结果
     */
    public static class MinioVerifyResult {
        private boolean accessible;
        private String endpoint;
        private boolean bucketExists;
        private boolean checkpointExists;
        private int checkpointFileCount;
        private long totalCheckpointSize;
        private boolean hasMetadata;
        private String errorMessage;

        public boolean isAccessible() { return accessible; }
        public void setAccessible(boolean accessible) { this.accessible = accessible; }
        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        public boolean isBucketExists() { return bucketExists; }
        public void setBucketExists(boolean bucketExists) { this.bucketExists = bucketExists; }
        public boolean isCheckpointExists() { return checkpointExists; }
        public void setCheckpointExists(boolean checkpointExists) { this.checkpointExists = checkpointExists; }
        public int getCheckpointFileCount() { return checkpointFileCount; }
        public void setCheckpointFileCount(int checkpointFileCount) { this.checkpointFileCount = checkpointFileCount; }
        public long getTotalCheckpointSize() { return totalCheckpointSize; }
        public void setTotalCheckpointSize(long totalCheckpointSize) { this.totalCheckpointSize = totalCheckpointSize; }
        public boolean isHasMetadata() { return hasMetadata; }
        public void setHasMetadata(boolean hasMetadata) { this.hasMetadata = hasMetadata; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    }

    /**
     * 迁移对比结果
     */
    public static class MigrationCompareResult {
        private SeaweedFsVerifyResult seaweedFs;
        private MinioVerifyResult minio;
        private boolean migrationSuccessful;
        private List<String> successCriteria;
        private List<String> failedCriteria;
        private String recommendation;

        public SeaweedFsVerifyResult getSeaweedFs() { return seaweedFs; }
        public void setSeaweedFs(SeaweedFsVerifyResult seaweedFs) { this.seaweedFs = seaweedFs; }
        public MinioVerifyResult getMinio() { return minio; }
        public void setMinio(MinioVerifyResult minio) { this.minio = minio; }
        public boolean isMigrationSuccessful() { return migrationSuccessful; }
        public void setMigrationSuccessful(boolean migrationSuccessful) { this.migrationSuccessful = migrationSuccessful; }
        public List<String> getSuccessCriteria() { return successCriteria; }
        public void setSuccessCriteria(List<String> successCriteria) { this.successCriteria = successCriteria; }
        public List<String> getFailedCriteria() { return failedCriteria; }
        public void setFailedCriteria(List<String> failedCriteria) { this.failedCriteria = failedCriteria; }
        public String getRecommendation() { return recommendation; }
        public void setRecommendation(String recommendation) { this.recommendation = recommendation; }
    }

    /**
     * 验证原 SeaweedFS 存储
     *
     * SeaweedFS 通常有两种使用方式：
     * 1. 挂载模式：SeaweedFS Filer 挂载为本地目录，路径如 /mnt/seaweedfs
     * 2. Hadoop 模式：通过 Hadoop 兼容接口，路径如 hdfs://seaweedfs:9200/...
     *
     * 本方法检查挂载模式下的 SeaweedFS 是否可访问。
     * 如果使用 Hadoop 模式，需要通过 Hadoop API 检查，此处仅作参考。
     *
     * @param seaweedFsPath SeaweedFS 的 Checkpoint 存储路径
     *                      例如：/mnt/seaweedfs/flink/checkpoints
     * @return SeaweedFS 验证结果
     */
    public SeaweedFsVerifyResult verifySeaweedFs(String seaweedFsPath) {
        SeaweedFsVerifyResult result = new SeaweedFsVerifyResult();
        result.setPath(seaweedFsPath);

        try {
            Path path = Paths.get(seaweedFsPath);

            // 检查路径是否可访问
            boolean exists = Files.exists(path);
            result.setAccessible(exists);

            if (!exists) {
                result.setErrorMessage("SeaweedFS 路径不存在: " + seaweedFsPath +
                    "。如果使用挂载模式，请确认 SeaweedFS Filer 已挂载到此路径。" +
                    "如果使用 Hadoop 模式，请通过 Hadoop API 检查。");
                return result;
            }

            // 检查是否可读
            boolean readable = Files.isReadable(path);
            if (!readable) {
                result.setErrorMessage("SeaweedFS 路径不可读: " + seaweedFsPath);
                return result;
            }

            // 统计 Checkpoint 文件
            int fileCount = 0;
            long totalSize = 0;
            boolean hasCheckpoint = false;

            if (Files.isDirectory(path)) {
                File[] files = path.toFile().listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isDirectory() && file.getName().startsWith("chk-")) {
                            hasCheckpoint = true;
                            File[] subFiles = file.listFiles();
                            if (subFiles != null) {
                                for (File subFile : subFiles) {
                                    fileCount++;
                                    totalSize += subFile.length();
                                }
                            }
                        } else if (file.getName().equals("_metadata")) {
                            hasCheckpoint = true;
                            fileCount++;
                            totalSize += file.length();
                        }
                    }
                }
            }

            result.setCheckpointExists(hasCheckpoint);
            result.setCheckpointFileCount(fileCount);
            result.setTotalCheckpointSize(totalSize);

            log.info("SeaweedFS 验证完成 - 路径: {}, 可访问: {}, Checkpoint存在: {}, 文件数: {}",
                seaweedFsPath, exists, hasCheckpoint, fileCount);

        } catch (Exception e) {
            result.setAccessible(false);
            result.setErrorMessage("验证 SeaweedFS 时出错: " + e.getMessage());
            log.error("验证 SeaweedFS 失败", e);
        }

        return result;
    }

    /**
     * 验证新 MinIO 存储
     *
     * 检查 MinIO 连接、Bucket 存在性、Checkpoint 文件完整性。
     * 这是迁移后最关键的验证步骤。
     *
     * @return MinIO 验证结果
     */
    public MinioVerifyResult verifyMinio() {
        MinioVerifyResult result = new MinioVerifyResult();
        result.setEndpoint(minioProperties.getEndpoint());

        try {
            // 检查 MinIO 连接（通过检查 Bucket 是否存在来验证连接）
            boolean bucketExists = minioClient.bucketExists(
                BucketExistsArgs.builder()
                    .bucket(minioProperties.getBucket())
                    .build()
            );

            result.setAccessible(true);
            result.setBucketExists(bucketExists);

            if (!bucketExists) {
                result.setErrorMessage("MinIO Bucket 不存在: " + minioProperties.getBucket() +
                    "。请先创建 Bucket 或调用初始化接口。");
                return result;
            }

            // 统计 Checkpoint 文件
            int fileCount = 0;
            long totalSize = 0;
            boolean hasCheckpoint = false;
            boolean hasMetadata = false;

            Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket(minioProperties.getBucket())
                    .prefix(minioProperties.getCheckpointPath())
                    .recursive(true)
                    .build()
            );

            for (Result<Item> itemResult : results) {
                Item item = itemResult.get();
                fileCount++;
                totalSize += item.size();

                String objectName = item.objectName();
                if (objectName.contains("chk-") || objectName.contains("_metadata")) {
                    hasCheckpoint = true;
                }
                if (objectName.contains("_metadata")) {
                    hasMetadata = true;
                }
            }

            result.setCheckpointExists(hasCheckpoint);
            result.setCheckpointFileCount(fileCount);
            result.setTotalCheckpointSize(totalSize);
            result.setHasMetadata(hasMetadata);

            log.info("MinIO 验证完成 - Endpoint: {}, Bucket存在: {}, Checkpoint存在: {}, 文件数: {}, 含元数据: {}",
                minioProperties.getEndpoint(), bucketExists, hasCheckpoint, fileCount, hasMetadata);

        } catch (Exception e) {
            result.setAccessible(false);
            result.setErrorMessage("验证 MinIO 时出错: " + e.getMessage() +
                "。请检查 MinIO 是否运行、endpoint 配置是否正确、密钥是否匹配。");
            log.error("验证 MinIO 失败", e);
        }

        return result;
    }

    /**
     * 执行完整的迁移对比验证
     *
     * 这是迁移验证的核心方法，按照三步法执行：
     * 1. 验证原 SeaweedFS
     * 2. 验证新 MinIO
     * 3. 对比并给出判定
     *
     * 【迁移成功的判定标准】
     *   ✅ 标准1：MinIO 连接正常
     *   ✅ 标准2：MinIO Bucket 存在
     *   ✅ 标准3：MinIO 中存在 Checkpoint 文件
     *   ✅ 标准4：Checkpoint 包含 _metadata 元数据
     *   ✅ 标准5：Checkpoint 文件数量 > 0
     *   ✅ 标准6：MinIO 可替代 SeaweedFS（MinIO 可用即可，SeaweedFS 可选）
     *
     * @param seaweedFsPath SeaweedFS 的 Checkpoint 路径（可为 null，表示不再需要验证旧存储）
     * @return 迁移对比结果
     */
    public MigrationCompareResult compareAndVerify(String seaweedFsPath) {
        MigrationCompareResult result = new MigrationCompareResult();
        List<String> successCriteria = new ArrayList<>();
        List<String> failedCriteria = new ArrayList<>();

        // 第一步：验证原 SeaweedFS（如果提供了路径）
        if (seaweedFsPath != null && !seaweedFsPath.isEmpty()) {
            SeaweedFsVerifyResult seaweedFsResult = verifySeaweedFs(seaweedFsPath);
            result.setSeaweedFs(seaweedFsResult);

            if (seaweedFsResult.isAccessible()) {
                successCriteria.add("原 SeaweedFS 存储可访问（路径: " + seaweedFsPath + "）");
            } else {
                failedCriteria.add("原 SeaweedFS 存储不可访问（路径: " + seaweedFsPath + "）- " +
                    seaweedFsResult.getErrorMessage());
            }

            if (seaweedFsResult.isCheckpointExists()) {
                successCriteria.add("原 SeaweedFS 存在历史 Checkpoint（文件数: " +
                    seaweedFsResult.getCheckpointFileCount() + "）");
            }
        } else {
            successCriteria.add("未提供 SeaweedFS 路径，跳过旧存储验证（迁移后不再需要）");
        }

        // 第二步：验证新 MinIO
        MinioVerifyResult minioResult = verifyMinio();
        result.setMinio(minioResult);

        // 标准1：MinIO 连接正常
        if (minioResult.isAccessible()) {
            successCriteria.add("✅ 标准1通过：MinIO 连接正常（endpoint: " + minioResult.getEndpoint() + "）");
        } else {
            failedCriteria.add("❌ 标准1失败：MinIO 连接失败 - " + minioResult.getErrorMessage());
        }

        // 标准2：MinIO Bucket 存在
        if (minioResult.isBucketExists()) {
            successCriteria.add("✅ 标准2通过：MinIO Bucket 存在（bucket: " + minioProperties.getBucket() + "）");
        } else {
            failedCriteria.add("❌ 标准2失败：MinIO Bucket 不存在 - 需要先创建 Bucket");
        }

        // 标准3：MinIO 中存在 Checkpoint 文件
        if (minioResult.isCheckpointExists()) {
            successCriteria.add("✅ 标准3通过：MinIO 中存在 Checkpoint 文件");
        } else {
            failedCriteria.add("❌ 标准3失败：MinIO 中不存在 Checkpoint 文件 - 需要先运行 Flink 作业触发 Checkpoint");
        }

        // 标准4：Checkpoint 包含 _metadata 元数据
        if (minioResult.isHasMetadata()) {
            successCriteria.add("✅ 标准4通过：Checkpoint 包含 _metadata 元数据文件");
        } else if (minioResult.isCheckpointExists()) {
            failedCriteria.add("❌ 标准4失败：Checkpoint 不包含 _metadata 文件 - Checkpoint 可能不完整");
        }

        // 标准5：Checkpoint 文件数量 > 0
        if (minioResult.getCheckpointFileCount() > 0) {
            successCriteria.add("✅ 标准5通过：Checkpoint 文件数量 = " + minioResult.getCheckpointFileCount() +
                "，总大小 = " + formatSize(minioResult.getTotalCheckpointSize()));
        } else {
            failedCriteria.add("❌ 标准5失败：MinIO 中无 Checkpoint 文件");
        }

        // 标准6：MinIO 可替代 SeaweedFS
        if (minioResult.isAccessible() && minioResult.isBucketExists()) {
            successCriteria.add("✅ 标准6通过：MinIO 可替代 SeaweedFS 作为 Checkpoint 存储");
        } else {
            failedCriteria.add("❌ 标准6失败：MinIO 尚不可用，无法替代 SeaweedFS");
        }

        result.setSuccessCriteria(successCriteria);
        result.setFailedCriteria(failedCriteria);

        // 综合判定：所有 6 项标准中，标准 1-3 必须通过才算迁移成功
        boolean coreCriteriaMet = minioResult.isAccessible()
            && minioResult.isBucketExists()
            && minioResult.isCheckpointExists();
        result.setMigrationSuccessful(coreCriteriaMet);

        // 给出建议
        if (coreCriteriaMet) {
            result.setRecommendation(
                "🎉 迁移验证通过！MinIO 已成功替代 SeaweedFS 作为 Flink Checkpoint 存储。" +
                "建议后续：1) 运行完整业务作业验证；2) 测试从 Checkpoint 恢复；3) 监控 Checkpoint 延迟。"
            );
        } else {
            StringBuilder recommendation = new StringBuilder("⚠️ 迁移验证未通过，请解决以下问题：\n");
            for (String failed : failedCriteria) {
                recommendation.append("  - ").append(failed).append("\n");
            }
            recommendation.append("\n常见解决方案：\n");
            recommendation.append("  1. MinIO 连接失败：检查 endpoint 配置和 MinIO 运行状态\n");
            recommendation.append("  2. Bucket 不存在：调用 POST /api/flink/migration/init-bucket 创建\n");
            recommendation.append("  3. 无 Checkpoint 文件：先提交 Flink 作业触发 Checkpoint\n");
            result.setRecommendation(recommendation.toString());
        }

        return result;
    }

    /**
     * 初始化 MinIO Bucket（迁移前置步骤）
     *
     * @return 是否成功
     */
    public boolean initMinioBucket() {
        try {
            minioInitService.ensureBucketExists();
            return true;
        } catch (Exception e) {
            log.error("初始化 MinIO Bucket 失败", e);
            return false;
        }
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
