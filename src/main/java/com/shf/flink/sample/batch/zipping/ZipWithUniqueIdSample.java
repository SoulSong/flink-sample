package com.shf.flink.sample.batch.zipping;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/3 02:49
 */
public class ZipWithUniqueIdSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataSet<String> input = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");

        DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithUniqueId(input);

        // maybe (0,G), (1,A), (2,H), (3,B), (5,C), (7,D), (9,E), (11,F)
        result.print();
    }

}
