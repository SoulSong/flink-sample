package com.shf.flink.sample.batch.connectors;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.mapred.JobConf;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/8 01:10
 */
public class MongoDbSample {

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create a MongodbInputFormat, using a Hadoop input format wrapper
        HadoopInputFormat<BSONWritable, BSONWritable> hadoopInputFormat =
                new HadoopInputFormat<>(new MongoInputFormat(),
                        BSONWritable.class, BSONWritable.class, new JobConf());

        // specify connection parameters, "mongodb://host:port/dbName.collectionName
        hadoopInputFormat.getJobConf().set("mongo.input.uri",
                "mongodb://127.0.0.1:27017/local.demo");

        DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hadoopInputFormat);
        input.print();
    }

}
