package com.shf.flink.sample.batch.connectors;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/6 17:17
 */
public class HdfsSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // hdfs://namenode_host:namenode_port/file_path
        env.readCsvFile("hdfs://127.0.0.1:9000/flink/sample/csv/person.csv")
                .ignoreFirstLine().includeFields("1100")
                .types(String.class, Integer.class)
                .print();
    }

}
