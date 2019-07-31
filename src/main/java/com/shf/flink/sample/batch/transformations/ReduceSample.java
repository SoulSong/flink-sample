package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 13:28
 */
public class ReduceSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<WC> dataSet = env.fromElements(new WC("foo", 1), new WC("bar", 10),
                new WC("foo", 2), new WC("bar", 12));

        // result : WC{word='bar', count=22} WC{word='foo', count=3}
        dataSet.groupBy("word")
                .reduce(new WordCounter())
                .print();

    }

    public static class WordCounter implements ReduceFunction<WC> {
        @Override
        public WC reduce(WC in1, WC in2) {
            return new WC(in1.word, in1.count + in2.count);
        }
    }

    public static class WC {
        public String word;
        public int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
