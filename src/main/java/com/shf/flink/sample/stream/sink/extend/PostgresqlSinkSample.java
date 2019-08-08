package com.shf.flink.sample.stream.sink.extend;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.shf.flink.sample.batch.Person;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.beans.PropertyVetoException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;

import static com.shf.flink.sample.batch.Constants.PERSON_CSV_FILE_PATH;

/**
 * Description:
 * Load csv by FileInputFormat and insert all records into postgreSQL
 *
 * @author: songhaifeng
 * @date: 2019/8/7 21:42
 */
public class PostgresqlSinkSample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PojoTypeInfo<Person> pojoType = (PojoTypeInfo<Person>) TypeExtractor.createTypeInfo(Person.class);
        String[] fields = new String[]{"name", "age", "sex", "address"};
        PojoCsvInputFormat<Person> personCsvInputFormat = new PojoCsvInputFormat<>(Path.fromLocalFile(Paths.get(PERSON_CSV_FILE_PATH).toFile()),
                pojoType, fields);
        personCsvInputFormat.setSkipFirstLineAsHeader(true);

        DataStreamSource<Person> dataStreamSource = env.createInput(personCsvInputFormat, pojoType);

        dataStreamSource.map(new MapFunction<Person, List<Person>>() {
            @Override
            public List<Person> map(Person value) throws Exception {
                return Collections.singletonList(value);
            }
        }).addSink(new SinkToPostgreSQL());

        env.execute();
    }

    static class SinkToPostgreSQL extends RichSinkFunction<List<Person>> {
        private PreparedStatement ps;
        private ComboPooledDataSource dataSource;
        private Connection connection;

        /**
         * Init connection
         *
         * @param parameters params
         * @throws Exception e
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            dataSource = new ComboPooledDataSource();
            connection = getConnection(dataSource);
            String sql = "insert into persons (name, age, sex, address) values (?,?,?,?);";
            ps = connection.prepareStatement(sql);
        }

        /**
         * release resources
         *
         * @throws Exception e
         */
        @Override
        public void close() throws Exception {
            if (null != connection) {
                connection.close();
            }
            if (null != ps) {
                ps.close();
            }
            if (null != dataSource) {
                dataSource.close();
            }
        }

        /**
         * @param value   value
         * @param context context
         * @throws Exception e
         */
        @Override
        public void invoke(List<Person> value, Context context) throws Exception {
            //遍历数据集合
            for (Person person : value) {
                ps.setString(1, person.getName());
                ps.setInt(2, person.getAge());
                ps.setString(3, person.getSex());
                ps.setString(4, person.getAddress());
                ps.addBatch();
            }
            int[] count = ps.executeBatch();
            System.out.println("Insert " + count.length + " rows into db.");
        }

        /**
         * Get connection from datasource
         *
         * @param dataSource dataSource
         * @return Connection
         * @throws PropertyVetoException e
         */
        private static Connection getConnection(ComboPooledDataSource dataSource) throws PropertyVetoException {
            dataSource.setDriverClass("com.mysql.jdbc.Driver");
            dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5433/postgres");
            dataSource.setUser("postgres");
            dataSource.setPassword("postgres");

            Connection con = null;
            try {
                con = dataSource.getConnection();
            } catch (Exception e) {
                System.out.println("Get connection has exception , msg = " + e.getMessage());
            }
            return con;
        }
    }
}
