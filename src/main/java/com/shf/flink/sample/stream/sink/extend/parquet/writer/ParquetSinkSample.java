package com.shf.flink.sample.stream.sink.extend.parquet.writer;

import com.shf.flink.sample.batch.Person;
import com.shf.flink.sample.stream.sink.extend.parquet.UserParquet;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.nio.file.Paths;

import static com.shf.flink.sample.batch.Constants.PERSON_CSV_FILE_PATH;

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
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);

        final PojoTypeInfo<Person> pojoType = (PojoTypeInfo<Person>) TypeExtractor.createTypeInfo(Person.class);
        String[] fields = new String[]{"name", "age", "sex", "address"};
        final PojoCsvInputFormat<Person> personCsvInputFormat = new PojoCsvInputFormat<>(Path.fromLocalFile(Paths.get(PERSON_CSV_FILE_PATH).toFile()),
                pojoType, fields);
        personCsvInputFormat.setSkipFirstLineAsHeader(true);

        DataStreamSource<Person> input = env.createInput(personCsvInputFormat, pojoType);

        String filePathToWrite = "file:///C:/Users/songhaifeng/Desktop/user.snappy.parquet";
        String schemaLocation = "D:/learnworkspace/fink-sample/src/main/resources/avro/user_parquet.avsc";
        BucketingSink<UserParquet> parquetSink = new BucketingSink<>(filePathToWrite);
        // format folders for partitions
        parquetSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HH-mm"));
        parquetSink.setWriter(new SinkParquetWriter<>(schemaLocation));
        parquetSink.setInProgressSuffix(".snappy.parquet");
        parquetSink.setPendingSuffix(".snappy.parquet");
        // When a bucket part file becomes larger than this size a new bucket part file is started and
        // the old one is closed (status pending - finished)
        parquetSink.setBatchSize(10 * 1024);
        input.map(new RichMapFunction<Person, UserParquet>() {
            @Override
            public UserParquet map(Person value) throws Exception {
                return new UserParquet(value.getName(), value.getAge(), value.getSex(), value.getAddress());
            }
        }).addSink(parquetSink).name("write_parquet");

        env.execute();

    }

}
