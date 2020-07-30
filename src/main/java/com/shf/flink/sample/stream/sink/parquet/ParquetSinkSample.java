package com.shf.flink.sample.stream.sink.parquet;

import com.shf.flink.sample.batch.UserParquet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

import java.nio.file.Paths;

import static com.shf.flink.sample.batch.Constants.OUT_ROOT_PATH;

/**
 * Description:
 * Write parquet file by customized writer.
 *
 * @author: songhaifeng
 * @date: 2019/8/10 22:57
 */
public class ParquetSinkSample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Streaming 到 Parquet 数据的生成是由Checkpoint触发的，因此必须设置Checkpoint为Enable状态
        env.enableCheckpointing(2 * 1000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<Long> input = env.generateSequence(1, 10000000);

        // 输出到 parquet 文件中
        String filePathToWrite = OUT_ROOT_PATH + "user_parquet";

        DateTimeBucketAssigner<UserParquet> bucketAssigner = new DateTimeBucketAssigner<>("yyyy-MM-dd--HH-mm");
        final StreamingFileSink<UserParquet> parquetSink = StreamingFileSink
                .forBulkFormat(Path.fromLocalFile(Paths.get(filePathToWrite).toFile()),
                        ParquetAvroWritersCustomize.forReflectRecord(UserParquet.class))
                .withOutputFileConfig(new OutputFileConfig("part", ".snappy.parquet"))
                // 基于时间的滚动策略，每秒滚动一个文件
                .withBucketCheckInterval(1000)
                // 按照分钟进行分区，决定目录结构
                .withBucketAssigner(bucketAssigner)
                .build();

        // 根据序列构造person对象
        input.map(new RichMapFunction<Long, UserParquet>() {
            @Override
            public UserParquet map(Long value) throws Exception {
                return new UserParquet("foo" + value, value.intValue(), "male", "foo road");
            }
        }).addSink(parquetSink).name("write_parquet");

        env.execute();
    }

}
