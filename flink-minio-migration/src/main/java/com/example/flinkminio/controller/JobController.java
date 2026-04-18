package com.example.flinkminio.controller;

import com.example.flinkminio.config.MinioProperties;
import com.example.flinkminio.model.JobSubmissionResult;
import com.example.flinkminio.service.CheckpointService;
import com.example.flinkminio.service.MigrationVerifyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 作业管理 REST 控制器
 *
 * 提供 REST API 用于：
 * 1. 提交 Flink 作业
 * 2. 查询 Checkpoint 状态
 * 3. 查看 MinIO 配置信息
 * 4. 迁移验证：对比 SeaweedFS 和 MinIO 的 Checkpoint 存储
 *
 * 通过 HTTP 接口可以方便地测试和验证 Flink + MinIO 的集成效果
 */
@RestController
@RequestMapping("/api/flink")
public class JobController {

    @Autowired
    private CheckpointService checkpointService;

    @Autowired
    private MinioProperties minioProperties;

    @Autowired
    private MigrationVerifyService migrationVerifyService;

    /**
     * 提交 WordCount 作业
     *
     * POST /api/flink/job/submit
     * 可选参数：inputFilePath（输入文件路径）
     *
     * @param inputFilePath 输入文件路径（可选）
     * @return 作业提交结果
     */
    @PostMapping("/job/submit")
    public ResponseEntity<JobSubmissionResult> submitJob(
            @RequestParam(required = false) String inputFilePath) {
        try {
            String jobName = checkpointService.submitWordCountJob(inputFilePath);
            JobSubmissionResult result = JobSubmissionResult.success(
                jobName, minioProperties.getFullCheckpointPath()
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            JobSubmissionResult result = JobSubmissionResult.failure(e.getMessage());
            return ResponseEntity.internalServerError().body(result);
        }
    }

    /**
     * 查询 Checkpoint 文件列表
     *
     * GET /api/flink/checkpoint/list
     *
     * @return MinIO 中的 Checkpoint 文件列表
     */
    @GetMapping("/checkpoint/list")
    public ResponseEntity<List<String>> listCheckpoints() {
        List<String> files = checkpointService.listCheckpointFiles();
        return ResponseEntity.ok(files);
    }

    /**
     * 检查 Checkpoint 是否存在
     *
     * GET /api/flink/checkpoint/exists
     *
     * @return 是否存在 Checkpoint
     */
    @GetMapping("/checkpoint/exists")
    public ResponseEntity<Map<String, Object>> checkpointExists() {
        boolean exists = checkpointService.checkpointExists();
        Map<String, Object> response = new HashMap<>();
        response.put("exists", exists);
        response.put("checkpointPath", minioProperties.getFullCheckpointPath());
        return ResponseEntity.ok(response);
    }

    /**
     * 获取当前 MinIO 配置信息
     *
     * GET /api/flink/config
     *
     * @return MinIO 配置信息（隐藏敏感信息）
     */
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("endpoint", minioProperties.getEndpoint());
        config.put("bucket", minioProperties.getBucket());
        config.put("checkpointPath", minioProperties.getFullCheckpointPath());
        config.put("accessKey", minioProperties.getAccessKey().substring(0, 3) + "***");
        return ResponseEntity.ok(config);
    }

    // ================================================================
    //  迁移验证接口
    //  以下接口用于对比验证 SeaweedFS 和 MinIO 的 Checkpoint 存储
    // ================================================================

    /**
     * 验证原 SeaweedFS 存储
     *
     * GET /api/flink/migration/verify-seaweedfs?path=/mnt/seaweedfs/flink/checkpoints
     *
     * 检查 SeaweedFS 挂载路径是否可访问，是否存在历史 Checkpoint。
     * 这是迁移前的"基准测试"，确认旧存储正常工作。
     *
     * 【如何验证原 SeaweedFS 正常】
     * 1. 调用此接口，传入 SeaweedFS 的 Checkpoint 存储路径
     * 2. 检查返回结果中 accessible = true（路径可访问）
     * 3. 检查 checkpointExists = true（存在历史 Checkpoint）
     * 4. 记录 checkpointFileCount 和 totalCheckpointSize 作为基准值
     *
     * @param path SeaweedFS 的 Checkpoint 存储路径
     * @return SeaweedFS 验证结果
     */
    @GetMapping("/migration/verify-seaweedfs")
    public ResponseEntity<Map<String, Object>> verifySeaweedFs(
            @RequestParam String path) {
        MigrationVerifyService.SeaweedFsVerifyResult result =
            migrationVerifyService.verifySeaweedFs(path);

        Map<String, Object> response = new HashMap<>();
        response.put("storage", "SeaweedFS");
        response.put("mode", "mount");
        response.put("path", result.getPath());
        response.put("accessible", result.isAccessible());
        response.put("checkpointExists", result.isCheckpointExists());
        response.put("checkpointFileCount", result.getCheckpointFileCount());
        response.put("totalCheckpointSize", result.getTotalCheckpointSize());
        response.put("totalCheckpointSizeHuman", formatSize(result.getTotalCheckpointSize()));
        if (result.getErrorMessage() != null) {
            response.put("errorMessage", result.getErrorMessage());
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 验证 SeaweedFS Filer（HTTP API 模式）
     *
     * GET /api/flink/migration/verify-seaweedfs-filer?filerUrl=http://filer:8888&checkpointPath=/flink/checkpoints
     *
     * 通过 HTTP API 访问 SeaweedFS Filer，检查 Checkpoint 文件。
     * 这是大多数生产环境使用的方式。
     *
     * 【使用场景】
     * 当 SeaweedFS 不是挂载模式，而是通过 Filer HTTP API 访问时，
     * 使用此接口验证。需要提供 Filer 地址和 Checkpoint 路径。
     *
     * 【参数说明】
     * - filerUrl: SeaweedFS Filer 的 HTTP API 地址
     *   示例：http://seaweedfs-dev-filer.geip-xa-dev-gdmp:8888
     * - checkpointPath: Checkpoint 在 SeaweedFS 中的存储路径
     *   示例：/flink/checkpoints
     *
     * @param filerUrl Filer 地址
     * @param checkpointPath Checkpoint 路径
     * @return SeaweedFS 验证结果
     */
    @GetMapping("/migration/verify-seaweedfs-filer")
    public ResponseEntity<Map<String, Object>> verifySeaweedFsFiler(
            @RequestParam String filerUrl,
            @RequestParam(defaultValue = "/flink/checkpoints") String checkpointPath) {

        MigrationVerifyService.SeaweedFsVerifyResult result =
            migrationVerifyService.verifySeaweedFsFiler(filerUrl, checkpointPath);

        Map<String, Object> response = new HashMap<>();
        response.put("storage", "SeaweedFS");
        response.put("mode", "filer-http-api");
        response.put("filerUrl", filerUrl);
        response.put("checkpointPath", checkpointPath);
        response.put("fullUrl", result.getPath());
        response.put("accessible", result.isAccessible());
        response.put("checkpointExists", result.isCheckpointExists());
        response.put("checkpointFileCount", result.getCheckpointFileCount());
        response.put("totalCheckpointSize", result.getTotalCheckpointSize());
        response.put("totalCheckpointSizeHuman", formatSize(result.getTotalCheckpointSize()));
        if (result.getErrorMessage() != null) {
            response.put("errorMessage", result.getErrorMessage());
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 使用配置文件验证 SeaweedFS
     *
     * GET /api/flink/migration/verify-seaweedfs-config
     *
     * 使用 application.yml 中配置的 SeaweedFS 信息进行验证。
     * 需要先在配置文件中设置：
     * seaweedfs:
     *   enabled: true
     *   filer-url: http://seaweedfs-filer:8888
     *   checkpoint-path: /flink/checkpoints
     *
     * @return SeaweedFS 验证结果
     */
    @GetMapping("/migration/verify-seaweedfs-config")
    public ResponseEntity<Map<String, Object>> verifySeaweedFsFromConfig() {
        MigrationVerifyService.SeaweedFsVerifyResult result =
            migrationVerifyService.verifySeaweedFsFromConfig();

        Map<String, Object> response = new HashMap<>();
        response.put("storage", "SeaweedFS");
        response.put("mode", "config-based");

        if (result == null) {
            response.put("accessible", false);
            response.put("errorMessage",
                "SeaweedFS 未配置或未启用。请在 application.yml 中设置:\n" +
                "seaweedfs:\n" +
                "  enabled: true\n" +
                "  filer-url: http://your-filer:8888\n" +
                "  checkpoint-path: /flink/checkpoints");
            return ResponseEntity.ok(response);
        }

        response.put("accessible", result.isAccessible());
        response.put("checkpointExists", result.isCheckpointExists());
        response.put("checkpointFileCount", result.getCheckpointFileCount());
        response.put("totalCheckpointSize", result.getTotalCheckpointSize());
        response.put("totalCheckpointSizeHuman", formatSize(result.getTotalCheckpointSize()));
        if (result.getErrorMessage() != null) {
            response.put("errorMessage", result.getErrorMessage());
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 验证新 MinIO 存储
     *
     * GET /api/flink/migration/verify-minio
     *
     * 检查 MinIO 连接、Bucket 存在性、Checkpoint 文件完整性。
     * 这是迁移后的"验收测试"，确认新存储正常工作。
     *
     * 【如何验证新 MinIO 正常】
     * 1. 调用此接口
     * 2. 检查返回结果中 accessible = true（MinIO 连接正常）
     * 3. 检查 bucketExists = true（Bucket 已创建）
     * 4. 检查 checkpointExists = true（Checkpoint 已写入）
     * 5. 检查 hasMetadata = true（Checkpoint 元数据完整）
     * 6. 记录 checkpointFileCount 和 totalCheckpointSize
     *
     * @return MinIO 验证结果
     */
    @GetMapping("/migration/verify-minio")
    public ResponseEntity<Map<String, Object>> verifyMinio() {
        MigrationVerifyService.MinioVerifyResult result =
            migrationVerifyService.verifyMinio();

        Map<String, Object> response = new HashMap<>();
        response.put("storage", "MinIO");
        response.put("endpoint", result.getEndpoint());
        response.put("accessible", result.isAccessible());
        response.put("bucketExists", result.isBucketExists());
        response.put("checkpointExists", result.isCheckpointExists());
        response.put("hasMetadata", result.isHasMetadata());
        response.put("checkpointFileCount", result.getCheckpointFileCount());
        response.put("totalCheckpointSize", result.getTotalCheckpointSize());
        response.put("totalCheckpointSizeHuman", formatSize(result.getTotalCheckpointSize()));
        if (result.getErrorMessage() != null) {
            response.put("errorMessage", result.getErrorMessage());
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 执行完整的迁移对比验证
     *
     * GET /api/flink/migration/compare?seaweedFsPath=/mnt/seaweedfs/flink/checkpoints
     *
     * 这是迁移验证的核心接口，按照三步法执行：
     * 1. 验证原 SeaweedFS（如果提供了路径）
     * 2. 验证新 MinIO
     * 3. 对比并给出判定
     *
     * 【如何对比和判断迁移成功】
     *
     * 迁移成功的 6 项标准：
     *   ✅ 标准1：MinIO 连接正常
     *   ✅ 标准2：MinIO Bucket 存在
     *   ✅ 标准3：MinIO 中存在 Checkpoint 文件
     *   ✅ 标准4：Checkpoint 包含 _metadata 元数据
     *   ✅ 标准5：Checkpoint 文件数量 > 0
     *   ✅ 标准6：MinIO 可替代 SeaweedFS
     *
     * 其中标准 1-3 为核心标准，必须全部通过才算迁移成功。
     * 标准 4-6 为增强标准，通过则表示迁移质量更高。
     *
     * @param seaweedFsPath SeaweedFS 的 Checkpoint 路径（可选，不传则跳过旧存储验证）
     * @return 迁移对比结果
     */
    @GetMapping("/migration/compare")
    public ResponseEntity<Map<String, Object>> compareMigration(
            @RequestParam(required = false) String seaweedFsPath) {

        MigrationVerifyService.MigrationCompareResult result =
            migrationVerifyService.compareAndVerify(seaweedFsPath);

        Map<String, Object> response = new HashMap<>();
        response.put("migrationSuccessful", result.isMigrationSuccessful());
        response.put("recommendation", result.getRecommendation());
        response.put("successCriteria", result.getSuccessCriteria());
        response.put("failedCriteria", result.getFailedCriteria());

        // SeaweedFS 验证结果
        if (result.getSeaweedFs() != null) {
            MigrationVerifyService.SeaweedFsVerifyResult sw = result.getSeaweedFs();
            Map<String, Object> seaweedFsInfo = new HashMap<>();
            seaweedFsInfo.put("path", sw.getPath());
            seaweedFsInfo.put("accessible", sw.isAccessible());
            seaweedFsInfo.put("checkpointExists", sw.isCheckpointExists());
            seaweedFsInfo.put("checkpointFileCount", sw.getCheckpointFileCount());
            seaweedFsInfo.put("totalCheckpointSize", sw.getTotalCheckpointSize());
            if (sw.getErrorMessage() != null) {
                seaweedFsInfo.put("errorMessage", sw.getErrorMessage());
            }
            response.put("seaweedFs", seaweedFsInfo);
        }

        // MinIO 验证结果
        if (result.getMinio() != null) {
            MigrationVerifyService.MinioVerifyResult mo = result.getMinio();
            Map<String, Object> minioInfo = new HashMap<>();
            minioInfo.put("endpoint", mo.getEndpoint());
            minioInfo.put("accessible", mo.isAccessible());
            minioInfo.put("bucketExists", mo.isBucketExists());
            minioInfo.put("checkpointExists", mo.isCheckpointExists());
            minioInfo.put("hasMetadata", mo.isHasMetadata());
            minioInfo.put("checkpointFileCount", mo.getCheckpointFileCount());
            minioInfo.put("totalCheckpointSize", mo.getTotalCheckpointSize());
            if (mo.getErrorMessage() != null) {
                minioInfo.put("errorMessage", mo.getErrorMessage());
            }
            response.put("minio", minioInfo);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 初始化 MinIO Bucket（迁移前置步骤）
     *
     * POST /api/flink/migration/init-bucket
     *
     * 在迁移前需要先创建 MinIO Bucket，此接口自动完成创建。
     *
     * @return 是否成功
     */
    @PostMapping("/migration/init-bucket")
    public ResponseEntity<Map<String, Object>> initMinioBucket() {
        boolean success = migrationVerifyService.initMinioBucket();

        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        response.put("bucket", minioProperties.getBucket());
        if (success) {
            response.put("message", "Bucket 已就绪（已存在或创建成功）");
        } else {
            response.put("message", "Bucket 初始化失败，请检查 MinIO 连接");
        }

        return success ? ResponseEntity.ok(response) : ResponseEntity.internalServerError().body(response);
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
