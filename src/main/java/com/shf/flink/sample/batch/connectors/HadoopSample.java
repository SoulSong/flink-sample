package com.shf.flink.sample.batch.connectors;

import com.alibaba.fastjson.JSONObject;
import com.shf.flink.sample.batch.Person;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/3 03:03
 */
public class HadoopSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set up the Hadoop TextInputFormat.
        Job job = Job.getInstance();
        HadoopInputFormat<LongWritable, Text> hadoopInputFormat =
                new HadoopInputFormat<>(new TextInputFormat(),
                        LongWritable.class, Text.class, new JobConf());

        TextInputFormat.addInputPath(hadoopInputFormat.getJobConf(),
                new Path("hdfs://127.0.0.1:9000/flink/sample/csv/person.csv"));

        // Read data using the Hadoop TextInputFormat.
        DataSet<Tuple2<LongWritable, Text>> input = env.createInput(hadoopInputFormat);

        // Emit data using the Hadoop TextOutputFormat.
        DataSet<Person> output = input.filter(tuple -> tuple.f0.compareTo(new LongWritable(0L)) != 0)
                .flatMap(new RichFlatMapFunction<Tuple2<LongWritable, Text>, Person>() {
                    @Override
                    public void flatMap(Tuple2<LongWritable, Text> value, Collector<Person> out) throws Exception {
                        Text text = value.f1;
                        String[] strs = text.toString().split(",");
                        out.collect(new Person(strs[0], Integer.parseInt(strs[1]), strs[2], strs[3]));
                    }
                });

        output.writeAsFormattedText("hdfs://127.0.0.1:9000/flink/sample/txt/person.txt", FileSystem.WriteMode.OVERWRITE,
                new TextOutputFormat.TextFormatter<Person>() {
            @Override
            public String format(Person value) {
                return JSONObject.toJSONString(value);
            }
        });
        env.execute();
    }
}
