package com.shf.flink.sample.batch.semantic.annotations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Description:
 * 被NonForwardedFields指定的字段将必须参与到函数计算过程中，并产生新的结果进行输出，但其可以不转发到output中；其余字段均保持原有位置直接转发到output中。
 * Function的Input和outPut必须是相同类型。
 *
 * @author: songhaifeng
 * @date: 2019/8/6 00:13
 */
public class NonForwardedFieldsSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(Tuple2.of(1, 10),
                Tuple2.of(2, 12),
                Tuple2.of(3, 14),
                Tuple2.of(4, 16));

        // (1,5)
        // (2,6)
        // (3,7)
        // (4,8)
        input.map(new MapFunctionSample()).print();
    }

    /**
     * second field is not forwarded, and must be participated in calculation.
     */
    @FunctionAnnotation.NonForwardedFields("f1")
    public static class MapFunctionSample implements
            MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> val) {
            return new Tuple2<>(val.f0, val.f1 / 2);
        }
    }
}
