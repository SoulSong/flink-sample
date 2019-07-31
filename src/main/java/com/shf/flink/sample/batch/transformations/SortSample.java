package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 14:15
 */
public class SortSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<ReduceSample.WC> dataSet = env.fromElements(new ReduceSample.WC("foo", 1),
                new ReduceSample.WC("bar", 16),
                new ReduceSample.WC("foo", 2),
                new ReduceSample.WC("bar", 12));

        UnsortedGrouping<Tuple2<String, Integer>> unsortedGrouping = dataSet.map(new MapFunction<ReduceSample.WC, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(ReduceSample.WC value) throws Exception {
                return Tuple2.of(value.word, value.count);
            }
        }).groupBy(0);

        // (bar,12)
        // (bar,16)
        // (foo,1)
        // (foo,2)
        System.out.println("order asc");
        unsortedGrouping.sortGroup(1, Order.ASCENDING)
                .reduceGroup(new SampleRichGroupReduceFunction()).print();

        // (bar,16)
        // (bar,12)
        // (foo,2)
        // (foo,1)
        System.out.println("order desc");
        unsortedGrouping.sortGroup(1, Order.DESCENDING)
                .reduceGroup(new SampleRichGroupReduceFunction()).print();
    }


    public static class SampleRichGroupReduceFunction extends RichGroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
            String key = null;
            for (Tuple2<String, Integer> value : values) {
                if (StringUtils.isNullOrWhitespaceOnly(key)) {
                    key = value.f0;
                }
                out.collect(Tuple2.of(key, value.f1));
            }
        }
    }
}
