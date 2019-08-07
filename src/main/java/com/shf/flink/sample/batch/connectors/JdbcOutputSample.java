package com.shf.flink.sample.batch.connectors;

import com.shf.flink.sample.batch.Person;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.types.Row;

/**
 * Description:
 *  <pre>
 *             CREATE TABLE persons (
 *                 name                 varchar(100)  ,
 *                 age                  integer  ,
 *                 sex                  varchar(100)  ,
 *                 address              varchar(100)
 *              )
 *  </pre>
 * @author: songhaifeng
 * @date: 2019/8/6 16:50
 */
public class JdbcOutputSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final String csvFilePath = "D:/learnworkspace/fink-sample/src/main/resources/sample/person.csv";

        DataSet<Person> personPojos = env.readCsvFile(csvFilePath).ignoreFirstLine()
                .pojoType(Person.class, "name", "age", "sex", "address");

        // build and configure OutputFormat
        JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl("jdbc:postgresql://127.0.0.1:5433/postgres")
                .setUsername("postgres").setPassword("postgres")
                .setQuery("insert into persons (name, age, sex, address) values (?,?,?,?)")
                .finish();

        // write dataSet to a relational database
        DataSet<Row> personDataSet= personPojos.map(new RichMapFunction<Person, Row>() {
            @Override
            public Row map(Person value) throws Exception {
                Row row = new Row(4);
                row.setField(0, value.getName());
                row.setField(1, value.getAge());
                row.setField(2, value.getSex());
                row.setField(3, value.getAddress());
                return row;
            }
        });

        personDataSet.output(jdbcOutputFormat);

        env.execute();
    }

}
