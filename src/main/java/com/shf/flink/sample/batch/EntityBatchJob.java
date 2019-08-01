package com.shf.flink.sample.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * Description:
 * Calculate the maximum age for each name from the customized person entity.
 * /bin/flink run -c com.shf.flink.sample.batch.EntityBatchJob D:\learnworkspace\fink-sample\target\fink-sample-Develop.jar
 *
 * @author: songhaifeng
 * @date: 2019/7/30 22:41
 */
public class EntityBatchJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Person> dataSource = env.fromElements(new Person("foo", 15),
                new Person("bar", 16), new Person("Foo", 25),
                new Person("car", 12), new Person("foo", 10),
                new Person("Bar", 11));
        dataSource.print();

        // try with FlatMapFunction
        DataSet<Tuple2<String, Integer>> counts = dataSource.flatMap(new FlatMapFunction<Person, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Person value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value.getName().toLowerCase(), value.getAge()));
            }
        }).groupBy(0).max(1);

        counts.print();

        // try with KeySelector and RichGroupReduceFunction
        dataSource.groupBy((KeySelector<Person, String>) value -> value.getName().toLowerCase())
                .reduceGroup(new RichGroupReduceFunction<Person, Tuple2<String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Person> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int maxAge = -9999;
                        String name = null;
                        for (Person value : values) {
                            if (StringUtils.isNullOrWhitespaceOnly(name)) {
                                name = value.getName();
                            }
                            if (value.getAge() > maxAge) {
                                maxAge = value.getAge();
                            }
                        }
                        out.collect(Tuple2.of(name, maxAge));
                    }
                }).print();
    }

    private static class Person {
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
