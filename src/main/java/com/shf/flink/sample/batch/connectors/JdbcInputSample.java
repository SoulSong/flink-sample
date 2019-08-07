package com.shf.flink.sample.batch.connectors;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/7 21:10
 */
public class JdbcInputSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl("jdbc:postgresql://127.0.0.1:5433/postgres")
                .setUsername("postgres")
                .setPassword("postgres")
                .setQuery("select name, age, sex, address from persons")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO))
                .finish();

        DataSet<Row> dbData =
                env.createInput(jdbcInputFormat);

        dbData.print();
    }

}
