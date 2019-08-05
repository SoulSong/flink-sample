package com.shf.flink.sample.batch.zipping;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/3 02:28
 */
public class ZipWithDenseIndexSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");

        DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithIndex(in);

        // (0,A)
        // (1,B)
        // (2,C)
        // (3,D)
        // (4,E)
        // (5,F)
        // (6,G)
        // (7,H)
        result.print();
    }

}
