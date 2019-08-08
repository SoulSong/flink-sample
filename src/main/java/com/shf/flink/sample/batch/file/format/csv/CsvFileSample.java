package com.shf.flink.sample.batch.file.format.csv;

import com.shf.flink.sample.batch.Person;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import static com.shf.flink.sample.batch.Constants.PERSON_CSV_FILE_PATH;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/6 17:23
 */
public class CsvFileSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read a CSV file with four fields, taking only the first and second field
        DataSet<Tuple2<String, Integer>> person = env.readCsvFile(PERSON_CSV_FILE_PATH)
                .ignoreFirstLine().includeFields("1100")
                .types(String.class, Integer.class);

        person.print();


        // read a CSV file with four fields into a POJO (Person.class) with corresponding fields
        // rule of POJO
        /*
        <pre>The class is public and standalone (no non-static inner class)
            The class has a public no-argument constructor
            All non-static, non-transient fields in the class (and all superclasses) are either public (and non-final) or
            have a public getter- and a setter- method that follows the Java beans naming conventions for getters and setters.
        </pre>
         */
        DataSet<Person> personPojos = env.readCsvFile(PERSON_CSV_FILE_PATH).ignoreFirstLine()
                .pojoType(Person.class, "name", "age", "sex", "address");
        personPojos.print();
    }

}
