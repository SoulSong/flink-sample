package com.shf.flink.sample.batch.parameters;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/8 23:27
 */
public class GlobalJobParametersSample {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Configuration conf = new Configuration();
        conf.setInteger("limit", 2);
        env.getConfig().setGlobalJobParameters(conf);

        DataSet<Integer> toFilter = env.fromElements(1, 2, 3);

        toFilter.filter(new RichFilterFunction<Integer>() {
            int limit = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // retrieve global setting
                ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                Configuration globConf = (Configuration) globalParams;
                limit = globConf.getInteger("limit", 1);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value > limit;
            }
        }).print();
    }

}
