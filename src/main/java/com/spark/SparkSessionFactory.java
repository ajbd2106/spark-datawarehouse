package com.spark;

import com.postgresql.PostGreSqlConfig;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class SparkSessionFactory {


    public static SparkSession createHiveSparkSession(String master, String localName) {
        return SparkSession
                .builder()
                .appName(localName)
                .config("hive.metastore.uris", "thrift://localhost:20102")
                .config("hive.support.concurrency", "true")
                .config("hive.enforce.bucketing", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
                .master(master)
                .enableHiveSupport()
                .getOrCreate();

    }

    public static SparkSession createClassicSparkSession(String master, String localName) {
        return SparkSession
                .builder()
                .appName(localName)
                .master(master)
                //TODO: we put shuffle to 5 but this should be tuned...
                .config("spark.sql.shuffle.partitions", "5")
                .getOrCreate();
    }

    public static Map<String, String> getPostGreSqlOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", PostGreSqlConfig.getInstance().getUrlWithCredentials());
        options.put("driver", PostGreSqlConfig.getInstance().getDriver());

        return options;
    }

}
