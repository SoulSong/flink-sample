package com.shf.flink.sample.stream.source.parquet;

import com.shf.flink.sample.stream.sink.parquet.UserParquet;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import static com.shf.flink.sample.batch.Constants.OUT_ROOT_PATH;

/**
 * description :
 * 读取 parquet 文件
 *
 * @author songhaifeng
 * @date 2020/7/30 15:51
 * @see https://github.com/apache/flink/blob/master/flink-formats/flink-parquet/src/test/java/org/apache/flink/formats/parquet/ParquetPojoInputFormatTest.java
 */
public class ParquetSourceSample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2 * 1000, CheckpointingMode.EXACTLY_ONCE);

        // 需要指定至目录的最后一级
        String filePathToWrite = OUT_ROOT_PATH + "user_parquet/2020-07-30--19-19";
        ParquetPojoInputFormat<UserParquet> inputFormat = new ParquetPojoInputFormat<>(
                new Path(filePathToWrite), MessageTypeBuilderByAvro.build(UserParquet.class), (PojoTypeInfo<UserParquet>) Types.POJO(UserParquet.class));
        TypeInformation<UserParquet> pojoType = TypeExtractor.createTypeInfo(UserParquet.class);

        DataStreamSource<UserParquet> pojoDataSource = env.createInput(inputFormat, pojoType);
        // 标准输出行数据
        pojoDataSource.map(new RichMapFunction<UserParquet, String>() {
            @Override
            public String map(UserParquet value) throws Exception {
                return value.toString();
            }
        }).addSink(new PrintSinkFunction<>());
        env.execute("parquet-reader");
    }


    private static class MessageTypeBuilderByAvro {

        private static <T> MessageType build(Class<T> typeClass) {
            Schema avroSchema = ReflectData.get().getSchema(typeClass);
            return new AvroSchemaConverter().convert(avroSchema);
        }
    }
}
