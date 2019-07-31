package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 23:14
 */
public class UnionSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> input1 = env.fromElements(Tuple2.of(1, "foo"), Tuple2.of(2, "bar"));
        DataSet<Tuple2<Integer, String>> input2 = env.fromElements(Tuple2.of(3, "bar"), Tuple2.of(4, "boo"));

        // (1,foo)
        // (3,bar)
        // (2,bar)
        // (4,boo)
        input1.union(input2).print();

    }
}
