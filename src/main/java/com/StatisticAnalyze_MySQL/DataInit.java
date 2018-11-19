package com.StatisticAnalyze_MySQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DataInit {
    public static SparkConf Sconf;
    public static SparkSession sparkSession;
    public static JavaSparkContext sc;
    public static Properties properties = new Properties();
    public static String topic_URL = "jdbc:mysql://192.168.13.204:3306/jdyp_topic?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

    public static String ResultURL = "jdbc:mysql://192.168.13.204:3306/jdyp_depot?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

    static {
        Sconf = new SparkConf()
                .set("spark.executor.memory", "4000m")
                .set("spark.driver.memory", "1024m")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.cores.max", "2")
                .setAppName("sparkReadHbaseData")
                .setMaster("local[2]");

        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");

        sparkSession = SparkSession.builder().config(Sconf)
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
                .config("spark.sql.parquet.compression.codec", "snappy").getOrCreate();
        sc = new JavaSparkContext(sparkSession.sparkContext());

        properties.put("user", "jdyp");
        properties.put("password", "keyway123");
        properties.put("driver", "com.mysql.jdbc.Driver");
    }

}
