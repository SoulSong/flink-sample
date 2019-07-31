package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 15:13
 */
public class AggregateSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, String, Integer>> input = env.fromElements(Tuple3.of(1, "foo", 13),
                Tuple3.of(2, "bar", 10),
                Tuple3.of(4, "foo", 11),
                Tuple3.of(3, "bar", 15));

        // (3,bar,25)
        // (4,foo,24)
        input.groupBy(1)
                .aggregate(Aggregations.MAX, 0)
                .and(Aggregations.SUM, 2)
                .print();

        // (3,bar,25)
        // (4,foo,24)
        input.groupBy(1)
                .max(0)
                .andSum(2).print();

        // (2,bar,10)
        // (1,foo,13)
        input.groupBy(1)
                .minBy(0, 2)
                .print();

        // (1,foo,13)
        // (2,bar,10)
        input.distinct(1).sortPartition(2, Order.DESCENDING).print();

    }
}
