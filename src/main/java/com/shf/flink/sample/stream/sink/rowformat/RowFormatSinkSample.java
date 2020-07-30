package com.shf.flink.sample.stream.sink.rowformat;

import com.shf.flink.sample.batch.Person;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static com.shf.flink.sample.batch.Constants.OUT_ROOT_PATH;

/**
 * description :
 * 流数据以字符串形式写入文本文件
 *
 * @author songhaifeng
 * @date 2020/7/29 17:14
 */
public class RowFormatSinkSample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用 StreamingFileSink 时需要启用 Checkpoint ，每次做 Checkpoint 时写入完成。
        // 如果 Checkpoint 被禁用，部分文件（part file）将永远处于 'in-progress' 或 'pending' 状态，下游系统无法安全地读取。
        env.enableCheckpointing(2 * 1000, CheckpointingMode.EXACTLY_ONCE);

        // 模拟流数据
        DataStreamSource<Long> input = env.generateSequence(1, 10000000);

        // 输出到 text 文件中
        String filePathToWrite = OUT_ROOT_PATH + "user_txt";
        FileUtils.deleteDirectory(Paths.get(filePathToWrite).toFile());
        DateTimeBucketAssigner<String> bucketAssigner = new DateTimeBucketAssigner<>("yyyy-MM-dd--HH-mm");
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(filePathToWrite), new SimpleStringEncoder<String>("UTF-8"))
                // 基于时间的滚动策略，每秒滚动一个文件
                .withBucketCheckInterval(1000)
                // 按照分钟进行分区,决定文件目录结构
                .withBucketAssigner(bucketAssigner)
                .withOutputFileConfig(new OutputFileConfig("part", ".txt"))
                .withRollingPolicy(
                        // 滚动策略，该策略在以下三种情况下滚动处于 In-progress 状态的部分文件（part file）：
                        // 至少包含 5 秒的数据
                        // 最近 2 秒没有收到新的记录
                        // 文件大小达到 1GB （写入最后一条记录后）
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toSeconds(5))
                                .withInactivityInterval(TimeUnit.SECONDS.toSeconds(2))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        // 根据序列构造person实体并转换为string
        input.map(new RichMapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return new Person("foo" + value, value.intValue(), "foo", "foo").toString();
            }
        }).addSink(sink).name("write_txt");
        env.execute();
    }

}
