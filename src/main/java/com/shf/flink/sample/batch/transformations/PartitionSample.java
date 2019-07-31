package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 23:23
 */
public class PartitionSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> input = env.fromElements(Tuple2.of(1, "foo"), Tuple2.of(2, "bar"), Tuple2.of(1, "bar"));

        // (1,foo)
        // (2,bar)
        // (1,bar)
        input.print();

        /***************************Partition By Hash**************************/
        DataSet<Tuple2<Integer, String>> out = input.partitionByHash(0);

        // (1,foo)
        // (1,bar)
        // (2,bar)
        out.print();

        // hash-partition DataSet by String value and apply a MapPartition transformation.
        // (1foo,foo)
        // (1bar,bar)
        // (2bar,bar)
        out.mapPartition(new RichMapPartitionFunction<Tuple2<Integer, String>, Tuple2<String, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
                for (Tuple2<Integer, String> value : values) {
                    out.collect(Tuple2.of(value.f0 + value.f1, value.f1));
                }
            }
        }).print();


        /***************************Partition By Range**************************/
        // (1,foo)
        // (1,bar)
        // (2,bar)
        input.partitionByRange(0).print();

        /***************************Partition By Sort**************************/
        // (2,bar)
        // (1,bar)
        // (1,foo)
        input.sortPartition(1, Order.ASCENDING)
                .sortPartition(0, Order.DESCENDING)
                .print();
    }


}
