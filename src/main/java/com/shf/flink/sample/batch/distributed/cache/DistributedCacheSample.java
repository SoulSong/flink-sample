package com.shf.flink.sample.batch.distributed.cache;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static com.shf.flink.sample.batch.Constants.EMAIL_CSV_FILE_PATH;
import static com.shf.flink.sample.batch.Constants.PERSON_CSV_FILE_PATH;

/**
 * Description:
 * 对不部分高频文件可以通过分布式缓存的方式，将其放置在每台计算节点实例的本地task内存中，从而提升整个任务的执行效率。
 * 使用完缓存文件后，Flink会自动将文件从本地文件系统中删除。
 *
 * @author: songhaifeng
 * @date: 2019/8/6 17:10
 */
public class DistributedCacheSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.registerCachedFile("file:///" + EMAIL_CSV_FILE_PATH, "emails_csv");

        DataSet<Tuple2<String, Integer>> persons = env.readCsvFile("file:///" + PERSON_CSV_FILE_PATH)
                .ignoreFirstLine().includeFields("1100").types(String.class, Integer.class);

        persons.flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>>() {
            File emailCsv = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                emailCsv = getRuntimeContext().getDistributedCache().getFile("emails_csv");
            }

            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                if (null != emailCsv && emailCsv.exists()) {
                    List<String> suffixes = Files.newBufferedReader(emailCsv.toPath()).lines().collect(Collectors.toList());
                    suffixes.remove(0);
                    if (CollectionUtils.isNotEmpty(suffixes)) {
                        suffixes.forEach(suffix -> {
                            out.collect(Tuple3.of(value.f0, value.f0 + "@" + suffix, value.f1));
                        });
                    }
                }
            }
        }).withForwardedFields("f1->f2").writeAsCsv("C:/Users/songhaifeng/Desktop/11.csv");

        env.execute();

    }
}
