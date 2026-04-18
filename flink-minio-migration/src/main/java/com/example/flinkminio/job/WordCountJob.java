package com.example.flinkminio.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink 流处理作业示例：单词频率统计（WordCount）
 *
 * 本作业演示一个典型的 Flink 流处理场景：
 * 1. 从数据源读取文本
 * 2. 按单词分组统计频率
 * 3. 使用窗口聚合输出结果
 *
 * 关键点：本作业配置了 Checkpoint 存储到 MinIO（S3 协议），
 * 当作业失败时可以从 MinIO 中的 Checkpoint 恢复状态。
 *
 * 【SeaweedFS 对比】
 * 如果使用 SeaweedFS，数据源路径可能是：
 *   - file:///mnt/seaweedfs/data/input.txt（挂载模式）
 *   - hdfs://seaweedfs:9200/data/input.txt（Hadoop 兼容模式）
 *
 * 迁移到 MinIO 后，数据源路径变为：
 *   - s3://flink-data/input.txt（S3 协议）
 *
 * 但 Checkpoint 的迁移更为关键，因为 Checkpoint 是容错的核心。
 */
public class WordCountJob {

    private static final Logger log = LoggerFactory.getLogger(WordCountJob.class);

    /**
     * 作业名称，用于 Flink Web UI 中显示
     */
    public static final String JOB_NAME = "MinIO-Checkpoint-WordCount";

    /**
     * 执行 WordCount 流处理作业
     *
     * @param env Flink 流执行环境（已配置好 Checkpoint 到 MinIO）
     */
    public static void execute(StreamExecutionEnvironment env) throws Exception {
        execute(env, null);
    }

    /**
     * 执行 WordCount 流处理作业（指定输入文件路径）
     *
     * @param env          Flink 流执行环境
     * @param inputFilePath 输入文件路径，如果为 null 则使用内置数据源
     */
    public static void execute(StreamExecutionEnvironment env, String inputFilePath) throws Exception {
        log.info("开始执行 WordCount 作业，输入路径: {}", inputFilePath);

        DataStream<String> textStream;

        if (inputFilePath != null && !inputFilePath.isEmpty()) {
            // 从文件读取文本
            // 支持 s3://、file://、hdfs:// 等协议
            FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                    new TextLineInputFormat(),
                    new Path(inputFilePath)
                )
                .build();

            textStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
            );
        } else {
            // 使用内置数据源（方便演示和测试）
            // 在实际生产中，通常从 Kafka、Socket 或文件读取
            textStream = env.fromElements(
                "hello world flink minio",
                "flink checkpoint minio s3",
                "seaweedfs migrate to minio",
                "hello flink streaming",
                "minio s3 compatible storage",
                "flink state backend rocksdb",
                "checkpoint recovery minio",
                "hello world hello flink"
            );
        }

        // =====================================================
        // 数据处理流水线
        // =====================================================

        // 第一步：将每行文本拆分为 (单词, 1) 的元组
        DataStream<Tuple2<String, Integer>> wordPairs = textStream
            .flatMap(new Tokenizer());

        // 第二步：按单词分组，使用滚动窗口（每 5 秒一个窗口）
        // 在窗口内对计数求和
        DataStream<Tuple2<String, Integer>> wordCounts = wordPairs
            .keyBy(tuple -> tuple.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .reduce(new WordCountReducer());

        // 第三步：打印结果到控制台
        wordCounts.print("WordCount-Result");

        // 执行作业
        env.execute(JOB_NAME);
    }

    /**
     * 文本分词器：将一行文本拆分为多个 (单词, 1) 元组
     *
     * 例如："hello world" -> ("hello", 1), ("world", 1)
     *
     * FlatMapFunction 是 Flink 的基本转换算子之一：
     * - 输入一个元素，可以输出零个或多个元素
     * - 类似于 Java Stream 的 flatMap
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            // 将文本转为小写，按空格分割
            String[] words = line.toLowerCase().split("\\s+");

            for (String word : words) {
                // 过滤空字符串
                if (!word.isEmpty()) {
                    // 输出 (单词, 1) 元组
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }

    /**
     * 单词计数归约器：将相同单词的计数累加
     *
     * 例如：("hello", 1) + ("hello", 1) = ("hello", 2)
     *
     * ReduceFunction 是 Flink 的聚合算子：
     * - 将两个相同类型的值合并为一个
     * - 在窗口触发时执行
     */
    public static class WordCountReducer implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            // 将两个元组的计数相加
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }
}
