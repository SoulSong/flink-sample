package com.shf.flink.sample.batch.semantic.annotations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Description:
 * 通过ForwardedFields指定的字段不进行修改、不参与函数的计算逻辑，仅仅作为输出项
 *
 * @author: songhaifeng
 * @date: 2019/8/6 00:02
 */
public class ForwardedFieldsSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(Tuple2.of(1, 10),
                Tuple2.of(2, 12),
                Tuple2.of(4, 14),
                Tuple2.of(3, 16));

        // (foo,5,1)
        // (foo,6,2)
        // (foo,7,4)
        // (foo,8,3)
        input.map(new MapFunctionSample()).print();

        // Equivalent implementation
        input.map(new MapFunctionSample2()).withForwardedFields("f0->f2").print();

    }

    /**
     * Indicates that the first element in the original tuple is forwarded to the third element location in the output tuple
     */
    @FunctionAnnotation.ForwardedFields("f0->f2")
    public static class MapFunctionSample implements
            MapFunction<Tuple2<Integer, Integer>, Tuple3<String, Integer, Integer>> {
        @Override
        public Tuple3<String, Integer, Integer> map(Tuple2<Integer, Integer> val) {
            return new Tuple3<>("foo", val.f1 / 2, val.f0);
        }
    }

    public static class MapFunctionSample2 implements
            MapFunction<Tuple2<Integer, Integer>, Tuple3<String, Integer, Integer>> {
        @Override
        public Tuple3<String, Integer, Integer> map(Tuple2<Integer, Integer> val) {
            return new Tuple3<>("foo", val.f1 / 2, val.f0);
        }
    }
}
