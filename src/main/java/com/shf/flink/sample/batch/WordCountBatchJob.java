package com.shf.flink.sample.batch;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * Word count sample.
 * /bin/flink run -c com.shf.flink.sample.batch.WordCountBatchJob D:\learnworkspace\fink-sample\target\fink-sample-Develop.jar D:\learnworkspace\fink-sample\src\main\resources/sample/wordcount_sample.txt
 *
 * @author songhaifeng
 */
public class WordCountBatchJob {

    public static void main(String[] args) throws Exception {
        if (ArrayUtils.isEmpty(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(args[0]);
        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);
        counts.print();
    }

    public static class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            Stream.of(tokens).filter(StringUtils::isNotEmpty).forEach(token -> out.collect(Tuple2.of(token, 1)));
        }
    }
}
