package com.shf.flink.sample.batch.semantic.annotations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Description:
 * ReadFields 指定Function中需要读取以及参与函数计算的字段，在注解中被指定的字段将全部参与当前函数结果的运算过程，如条件判断、数值计算等。
 * 一定函数添加了ReadFields注解，则其中所有参与计算的字段均必须在注解中指明，否则会导致计算函数执行失败。
 *
 * @author: songhaifeng
 * @date: 2019/8/6 00:23
 */
public class ReadFieldsSample {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<Integer, Integer, Integer, Integer>> input = env.fromElements(Tuple4.of(1, 10, 100, 1000),
                Tuple4.of(42, 12, 120, 1200),
                Tuple4.of(5, 14, 140, 1400),
                Tuple4.of(42, 16, 160, 1600));

        // (1010,10)
        // (42,12)
        // (1410,14)
        // (42,16)
        input.map(new MapFunctionSample()).print();
    }

    /**
     * f0 and f3 are read and evaluated by the function.
     */
    @FunctionAnnotation.ReadFields("f0;f3")
    public static class MapFunctionSample implements
            MapFunction<Tuple4<Integer, Integer, Integer, Integer>,
                    Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple4<Integer, Integer, Integer, Integer> val) {
            if (val.f0 == 42) {
                return new Tuple2<>(val.f0, val.f1);
            } else {
                return new Tuple2<>(val.f3 + 10, val.f1);
            }
        }
    }
}
