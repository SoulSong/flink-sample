package com.shf.flink.sample.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
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
        DataSource<Person> dataSource = env.fromElements(new Person("foo", 15),
                new Person("bar", 16), new Person("Foo", 25),
                new Person("car", 12), new Person("foo", 10),
                new Person("Bar", 11));

        // try with FlatMapFunction
        DataSet<Tuple2<String, Integer>> counts = dataSource.flatMap(new FlatMapFunction<Person, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Person value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value.getName().toLowerCase(), value.getAge()));
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
                        out.collect(new Tuple2<>(name, maxAge));
                    }
                }).collect().forEach(tuple2 -> System.out.println(tuple2.f0 + ":" + tuple2.f1));
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

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
