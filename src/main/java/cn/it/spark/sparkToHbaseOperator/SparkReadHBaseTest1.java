package cn.it.spark.sparkToHbaseOperator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkReadHBaseTest1 {

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
        //配置spark
        SparkConf Sconf = new SparkConf()
                .set("spark.executor.memory", "1000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local[2]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "4");

        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        //HBASE连接  配置zookeeper  ....
        Configuration Hconf = HBaseConfiguration.create();
        Hconf.set("hbase.zookeeper.property.clientPort", "2181");
        Hconf.set("hbase.zookeeper.quorum", "10.3.10.134");
        Hconf.set("hbase.master", "10.3.10.134:16030");

        String tableName = "Ticket_Data";
        //设置查询的表名，开始rowkey，结束rowkey，列族
        Hconf.set(TableInputFormat.INPUT_TABLE, tableName);
        Hconf.set(TableInputFormat.SCAN_ROW_START, "2017-04-2712:17:5561618");
        Hconf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cf");

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                sc.newAPIHadoopRDD(Hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD<Ticket> dataRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Ticket>() {
            @Override
            public Ticket call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                Ticket t = new Ticket();
                Result result = new Result();
                t.setCI(v1._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("CI")).toString());
                t.setIMSI(v1._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("IMSI")).toString());
                t.setIMEI(v1._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("IMEI")).toString());
                t.setLAI(v1._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("LAI")).toString());
//                t.setrecode_type(v1._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("记录类型")).toString());
                return t;
            }
        });


        Dataset<Row> df = sparkSession.createDataFrame(dataRDD, Ticket.class);
        df.createOrReplaceTempView("Ticket");
        Dataset<Row> sqlDS = sparkSession.sql("select CI,IMSI,IMEI,LAI from Ticket ");
        sqlDS.show();
        JavaRDD<Row> rowJavaRDD = sqlDS.javaRDD();
        JavaRDD<Ticket> result = rowJavaRDD.map(new Function<Row, Ticket>() {
            @Override
            public Ticket call(Row v1) throws Exception {
                Ticket t = new Ticket();
                t.setCI(v1.getString(0));
                t.setIMSI(v1.getString(1));
                t.setIMEI(v1.getString(2));
                t.setLAI(v1.getString(3));
                return t;
            }
        });

        List<Ticket> collect = result.collect();
        for (Ticket row : collect) {
            System.out.println(row.getCI()+"IMSI"+row.getIMSI()+"IMEI:"+row.getIMEI() + "LAI"+ row.getLAI());
        }

    }


}
