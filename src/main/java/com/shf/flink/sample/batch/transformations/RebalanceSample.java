package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 23:15
 */
public class RebalanceSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> input = env.fromElements(Tuple2.of(1, "foo"), Tuple2.of(2, "bar"), Tuple2.of(1, "bar"));

        DataSet<Tuple2<String, String>> out = input.rebalance()
                .map(new MapFunction<Tuple2<Integer, String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(Tuple2<Integer, String> value) throws Exception {
                        return Tuple2.of(value.f0.toString(), value.f1);
                    }
                });

        // (1,foo)
        // (2,bar)
        // (1,bar)
        out.print();
    }
}
