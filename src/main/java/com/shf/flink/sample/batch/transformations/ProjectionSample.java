package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 11:30
 */
public class ProjectionSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Double, String>> in = env.fromElements(Tuple3.of(1, 1.1, "foo"), Tuple3.of(2, 2.2, "bar"));
        // converts Tuple3<Integer, Double, String> into Tuple2<String, Integer>
        DataSet<Tuple2<String, Integer>> out = in.project(2, 0);

        // (foo,1) (bar,2)
        out.print();
    }

}
