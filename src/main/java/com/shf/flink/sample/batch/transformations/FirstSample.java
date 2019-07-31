package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 23:40
 */
public class FirstSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> input = env.fromElements(Tuple2.of(1, "foo"), Tuple2.of(2, "bar"), Tuple2.of(1, "bar"));

        // (1,foo)
        // (2,bar)
        input.first(2).print();

        // (2,bar)
        // (1,foo)
        input.groupBy(1).first(1).print();

        // Return the first three elements of each String group ordered by the Integer field
        // (1,bar)
        // (1,foo)
        // (2,bar)
        input.groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .first(3)
                .print();
    }
}
