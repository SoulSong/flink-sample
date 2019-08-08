package com.shf.flink.sample.batch.connectors;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Description:
 * Configure both s3.access-key and s3.secret-key in Flink’s flink-conf.yaml:
 * s3.access-key: your-access-key
 * s3.secret-key: your-secret-key
 * <p>
 * - Dev：
 * 1、Must set env as follows:
 * FLINK_CONF_DIR： D:\flink-1.8.1-bin-scala_2.11\flink-1.8.1\conf
 * 2、Must initialize fileSystem:
 * FileSystem.initialize(GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR")));
 * <p>
 * - Prod：
 * 1、Must copy flink-s3-fs-hadoop-1.8.1.jar from flink/opt to flink/lib;
 * 2、Remove FileSystem.initialize logic;
 * 3、execute command:
 * /bin/flink run -c com.shf.flink.sample.batch.connectors.S3Sample D:\learnworkspace\fink-sample\target\fink-sample-Develop.jar
 *
 * <p>
 * https://ci.apache.org/projects/flink/flink-docs-release-1.8/ops/deployment/aws.html#shaded-hadooppresto-s3-file-systems-recommended
 *
 * @author: songhaifeng
 * @date: 2019/8/9 00:07
 */
public class S3Sample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // only need when running in IDE
        FileSystem.initialize(GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR")));

        DataSet<Tuple2<String, Integer>> ds = env.readCsvFile("s3a://s-chemical-fda-prod/songhaifeng/redshift/person.csv")
                .ignoreFirstLine().includeFields("1100")
                .types(String.class, Integer.class);
        ds.print();
    }
}
