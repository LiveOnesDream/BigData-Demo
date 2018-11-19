package com.kr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class spark2MysqlBigTb {
    public static void main(String[] args) {
        SparkConf Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "3");

        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sc);

        String ticketURL = "jdbc:mysql://192.168.13.204:3306/jdyp_topic?useSSL=false&useUnicode=true" +
                "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

        String sql = "(select * from bs_460) as T";
        String start = "45400000001000000001";
        String end = "45506800032000000592";
        Dataset ticketDT = sqlContext.read().format("jdbc").option("url", ticketURL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "jdyp").option("password", "keyway123")
                .option("dbtable", sql)
                .option("numPartitions", 20).option("partitionColumn", "ID").option("lowerBound",0).option("upperBound", 200000)
                .load();
        ticketDT.show();
    }
}
