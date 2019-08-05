package com.shf.flink.sample.batch.broadcast;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/6 16:43
 */
public class BroadcastSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1. The DataSet to be broadcast
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
        DataSet<String> data = env.fromElements("a", "b");

        data.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            Collection<Integer> broadcastSet = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. Access the broadcast DataSet as a Collection
                broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (CollectionUtils.isNotEmpty(broadcastSet)) {
                    broadcastSet.forEach(intValue -> out.collect(Tuple2.of(value, intValue)));
                }
            }

        })
                // 2. Broadcast the DataSet
                .withBroadcastSet(toBroadcast, "broadcastSetName")
                // (a,1)
                // (a,2)
                // (a,3)
                // (b,1)
                // (b,2)
                // (b,3)
                .print();
    }

}
