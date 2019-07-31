package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 23:02
 */
public class CoGroupFunctionSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, Integer>> iVals = env.fromElements(Tuple2.of("foo", 1),
                Tuple2.of("foo", 2),
                Tuple2.of("foo", 2),
                Tuple2.of("bar", 2));
        DataSet<Tuple2<String, Double>> dVals = env.fromElements(Tuple2.of("bar", 3.3d), Tuple2.of("foo", 4.4));

        // 6.6
        // 4.4
        // 8.8
        DataSet<Double> output = iVals.coGroup(dVals)
                .where(0)
                .equalTo(0)
                // apply CoGroup function on each pair of groups
                .with(new MyCoGrouper());
        output.print();
    }

    static class MyCoGrouper implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Double>, Double> {

        @Override
        public void coGroup(Iterable<Tuple2<String, Integer>> iVals,
                            Iterable<Tuple2<String, Double>> dVals,
                            Collector<Double> out) {

            Set<Integer> ints = new HashSet<>();

            // add all Integer values in group to set
            for (Tuple2<String, Integer> val : iVals) {
                ints.add(val.f1);
            }

            // multiply each Double value with each unique Integer values of group
            for (Tuple2<String, Double> val : dVals) {
                for (Integer i : ints) {
                    out.collect(val.f1 * i);
                }
            }
        }

    }
}
