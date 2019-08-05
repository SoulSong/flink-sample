package com.shf.flink.sample.batch.Iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/2 23:03
 */
public class BulkIterationsSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create initial IterativeDataSet
        int maxIterations = 10000;
        IterativeDataSet<Integer> initial = env.fromElements(2, 3).iterate(maxIterations);

        // 每个element会循环执行maxIterations次，且每次均会在上一次的返回值的基础上自增一，如此反复maxIterations次
        DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                return i + 1;
            }
        });

        // Iteratively transform the IterativeDataSet
        DataSet<Integer> count = initial.closeWith(iteration);

        // 10002
        // 10003
        count.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer count) throws Exception {
                return count;
            }
        }).print();
    }

}
