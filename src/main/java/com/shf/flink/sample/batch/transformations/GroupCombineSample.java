package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 14:46
 */
public class GroupCombineSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> input = env.fromElements("foo", "bar", "foo", "foo", "bar", "car");

        DataSet<Tuple2<String, Integer>> combinedWords = input.groupBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        })
                // count each group
                .combineGroup(new GroupCombineFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void combine(Iterable<String> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String key = null;
                        int count = 0;
                        for (String word : values) {
                            key = word;
                            count++;
                        }
                        // emit tuple with word and count
                        out.collect(Tuple2.of(key, count));
                    }

                });
        // (bar,2)
        // (car,1)
        // (foo,3)
        combinedWords.print();

        DataSet<Tuple2<String, Integer>> output = combinedWords
                // group by words again
                .groupBy(0)
                // group reduce with full data exchange
                .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    @Override
                    public void reduce(Iterable<Tuple2<String, Integer>> words, Collector<Tuple2<String, Integer>> out) {
                        String key = null;
                        int count = 0;

                        for (Tuple2<String, Integer> word : words) {
                            key = word.f0;
                            count++;
                        }
                        // emit tuple with word and count
                        out.collect(Tuple2.of(key, count));
                    }
                });
        // (bar,1)
        // (car,1)
        // (foo,1)
        output.print();

    }
}
