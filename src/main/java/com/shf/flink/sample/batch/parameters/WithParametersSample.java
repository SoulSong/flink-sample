package com.shf.flink.sample.batch.parameters;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/8 23:22
 */
public class WithParametersSample {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> toFilter = env.fromElements(1, 2, 3);

        Configuration config = new Configuration();
        config.setInteger("limit", 2);

        toFilter.filter(new RichFilterFunction<Integer>() {
            private int limit;

            @Override
            public void open(Configuration parameters) throws Exception {
                DelegatingConfiguration configuration = new DelegatingConfiguration(parameters, "");
                limit = configuration.getInteger("limit", 0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value > limit;
            }
        }).withParameters(config).print();


    }

}
