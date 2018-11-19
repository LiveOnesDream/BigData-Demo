package com.kr;

import com.sun.xml.internal.ws.client.HandlerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;


public class sparkHfile {
    private static Logger log = Logger.getLogger(sparkHfile.class);

    public static void main(String[] args) throws Exception {
        SparkConf Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "2");

        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sc);

        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        hconf.set("hbase.zookeeper.quorum", "10.3.10.134");
        hconf.set("hbase.master", "10.3.10.134:16030");
        String tableName = "";
        hconf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

        HTable table = new HTable(hconf, tableName);
        Job job = Job.getInstance(hconf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Result.class);
        HFileOutputFormat.configureIncrementalLoad(job, table);
        String hdfsPath = "hdfs://mycluster/raw/hfile/blog.txt";
        JavaRDD<String> lines = sc.textFile(hdfsPath);

        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd = lines.mapToPair(new PairFunction<String, ImmutableBytesWritable, KeyValue>() {
            public Tuple2<ImmutableBytesWritable, KeyValue> call(String v1) throws Exception {
                String[] tokens = v1.split(" ");
                String rowkey = tokens[0];
                String content = tokens[1];
                KeyValue keyValue = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("article"), Bytes.toBytes("value"), Bytes.toBytes(content));
                return new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue);
            }

        });
        String hfilePath = "hdfs://mycluster/hfile/blog.hfile";
        hfileRdd.saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat.class, hconf);

        //利用bulk load hfile
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(hconf);
        bulkLoader.doBulkLoad(new Path(hfilePath), table);



    }
}
