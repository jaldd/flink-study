package com.example.flinkminio.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class FlinkConfig {

    private static final Logger log = LoggerFactory.getLogger(FlinkConfig.class);

    private final MinioProperties minioProperties;

    public FlinkConfig(MinioProperties minioProperties) {
        this.minioProperties = minioProperties;
    }

    public StreamExecutionEnvironment createExecutionEnvironment() {
        org.apache.flink.configuration.Configuration flinkConfig = new org.apache.flink.configuration.Configuration();
        flinkConfig.setString("s3.endpoint", minioProperties.getEndpoint());
        flinkConfig.setString("s3.access-key", minioProperties.getAccessKey());
        flinkConfig.setString("s3.secret-key", minioProperties.getSecretKey());
        flinkConfig.setString("s3.path.style.access", String.valueOf(minioProperties.isS3PathStyleAccess()));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDBStateBackend);

        env.enableCheckpointing(30000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(600000);
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        String checkpointPath = minioProperties.getFullCheckpointPath();
        log.info("设置 Checkpoint 存储路径: {}", checkpointPath);
        checkpointConfig.setCheckpointStorage(checkpointPath);

        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS))
        );

        log.info("Flink 执行环境配置完成，Checkpoint 存储路径: {}", checkpointPath);
        return env;
    }
}
