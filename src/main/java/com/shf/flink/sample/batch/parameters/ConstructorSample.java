package com.shf.flink.sample.batch.parameters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/8 23:11
 */
public class ConstructorSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> toFilter = env.fromElements(1, 2, 3);

        toFilter.filter(new FilterSample(2)).print();
    }

    private static class FilterSample implements FilterFunction<Integer> {

        private final int limit;

        public FilterSample(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean filter(Integer value) throws Exception {
            return value > limit;
        }
    }
}
