package com.shf.flink.sample.batch.file.format.avro;

import com.shf.flink.sample.batch.Person;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroOutputFormat;

import static com.shf.flink.sample.batch.Constants.PERSON_CSV_FILE_PATH;

/**
 * Description:
 * Execute command 'java -jar /path/to/avro-tools-*.*.*.jar compile schema <schema file> <destination>'
 * to generate Pojo class.
 * <p>
 * Note that using the GenericData.Record type is possible with Flink, but not recommended.
 * Since the record contains the full schema, its very data intensive and thus probably slow to use.
 *
 * @author: songhaifeng
 * @date: 2019/8/8 01:09
 */
public class AvroFileSample {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read from csv
        DataSet<Person> personPojos = env.readCsvFile(PERSON_CSV_FILE_PATH).ignoreFirstLine()
                .pojoType(Person.class, "name", "age", "sex", "address");

        DataSet<User> userDs = personPojos.map(new RichMapFunction<Person, User>() {
            @Override
            public User map(Person value) throws Exception {
                return new User(value.getName(), value.getAge(), value.getSex(), value.getAddress());
            }
        });

        // Output
        final String avroFilePath = "C:/Users/songhaifeng/Desktop/user.avro";
        userDs.output(new AvroOutputFormat<>(new Path(avroFilePath), User.class));

        env.execute();

        // Input
        DataSet<User> input = env.createInput(new AvroInputFormat<>(new Path(avroFilePath), User.class));
        input.print();
    }

}
