package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 22:15
 */
public class CrossWithProjectionSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> input1 = env.fromElements(Tuple2.of(1, "foo"), Tuple2.of(2, "bar"));
        DataSet<Tuple2<Integer, String>> input2 = env.fromElements(Tuple2.of(3, "car"), Tuple2.of(4, "boo"));

        // ((1,foo),(3,car))
        // ((1,foo),(4,boo))
        // ((2,bar),(3,car))
        // ((2,bar),(4,boo))
        input1.cross(input2).print();

        // (1,foo,car)
        // (2,bar,car)
        // (1,foo,boo)
        // (2,bar,boo)
        input1.crossWithHuge(input2)
                // apply a projection (or any Cross function)
                .projectFirst(0, 1)
                .projectSecond(1)
                .print();
    }

}
