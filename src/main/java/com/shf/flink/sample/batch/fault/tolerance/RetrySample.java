package com.shf.flink.sample.batch.fault.tolerance;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 14:15
 */
public class RetrySample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // retry 5 times, 5000 milliseconds delay
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,5000));

        // (1)
        env.fromElements(Tuple1.of(1),Tuple1.of(1)).distinct(0).print();
    }

}
