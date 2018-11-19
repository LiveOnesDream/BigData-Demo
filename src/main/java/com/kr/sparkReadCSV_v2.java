package com.kr;

import com.databricks.spark.csv.util.TextFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.datanucleus.store.types.SCO;
import scala.Tuple2;

import java.io.File;
import java.util.*;


public class sparkReadCSV_v2 {

    static final Logger log = Logger.getLogger(sparkReadCSV_v2.class);

    public static String filePath = "C:\\Users\\Administrator\\Desktop\\科瑞\\账单2\\涉毒交易明细1.csv";
    public static String charset = "utf-8";

    public static void main(String[] args) throws Exception {

        SparkConf Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadCSV_v2")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "3");

//        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();

        SparkContext sc = new SparkContext(Sconf);
        SQLContext sqlContext = new SQLContext(sc);
//        SparkContext sc = new SparkContext(Sconf);
//        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
//        SQLContext sqlContext = new SQLContext(sc);

//        String path = ExcelConvertCSV.exceltoCSV(filePath);
        String path = filePath;
//        String path = "F:\\csv\\13470849876姚远.csv";
//        Dataset<Row> csvDT = sparkSession.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").
//                option("header", true).
//                option("inferSchema", true). //自动推测字段类型
//                option("nullValue", "\\N").
//                option("delimiter", ",").
//                load(path);

        RDD<String> inputRDD = TextFile.withCharset(sc, path, charset);
        //获取值
        String header = inputRDD.first();
        //获取csv的字段 schema信息
        CSVRecord csvRecord = CSVParser.parse(header, CSVFormat.DEFAULT).getRecords().get(0);
        List<String> csvSchemaColumns = new ArrayList<>();
        Iterator<String> iterator = csvRecord.iterator();
        while (iterator.hasNext()) {
            String fileds = iterator.next();
            if (fileds.equals("记录类型")) {
                fileds = "record_type_one";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("记录类型")) {
                fileds = "record_type_two";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("呼叫日期")) {
                fileds = "call_date";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("呼叫时间")) {
                fileds = "call_time";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("呼叫时长")) {
                fileds = "call_duration";
                csvSchemaColumns.add(fileds);
            }

            if (fileds.equals("事件类型")) {
                fileds = "event_type_one";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("事件类型")) {
                fileds = "event_type_two";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("用户号码")) {
                fileds = "user_number";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("用户归属地")) {
                fileds = "user_affiliation";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("IMSI")) {
                fileds = "imsi";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("IMEI")) {
                fileds = "imei";
                csvSchemaColumns.add(fileds);
            }

            if (fileds.equals("对方号码")) {
                fileds = "other_number";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("对方归属地")) {
                fileds = "other_affiliation";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("LAI")) {
                fileds = "lai";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("CI")) {
                fileds = "ci";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("基站地址")) {
                fileds = "station_address_one";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("基站地址")) {
                fileds = "station_address_two";
                csvSchemaColumns.add(fileds);
            }
            if (fileds.equals("通话所在地区号")) {
                fileds = "call_address";
                csvSchemaColumns.add(fileds);
            }

            System.out.println(fileds);
        }

        List<Map<String, String>> rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String, String>>() {
            @Override
            public Map<String, String> call(String v1) throws Exception {
                String[] splited = v1.split(",");
                Map<String, String> map = new HashMap<>();
                for (int i = 0; i < splited.length; i++) {
                    if (!csvSchemaColumns.get(i).equals(splited[i])) {
                        map.put(csvSchemaColumns.get(i), splited[i]);
                    } else {
                        map.put("null", "null");
                    }
                }
                return map;
            }
        }).collect();

//        rdd.foreachPartition(new VoidFunction<Iterator>() {
//            @Override
//            public void call(Iterator iterator) throws Exception {
//                while (iterator.hasNext()) {
//                    System.out.println(iterator.next());
//                }
//            }
//        });

//        rdd.saveAsTextFile("F:\\csv\\ccccc");

        //按行读取csv文件数据，生成key，value对
//        JavaRDD rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String, String>>() {
//            @Override
//            public Map<String, String> call(String v1) throws Exception {
//                Map<String, String> map = new HashMap<>();
//                String[] fields = v1.split(",");
//                for (int i = 0; i < csvSchemaColumns.size(); i++) {
//                    if (!csvSchemaColumns.get(i).equals(fields[i])) {
//                        map.put(csvSchemaColumns.get(i), fields[i]);
//                    } else {
//                        map.put("null", "null");
//                    }
//                }
//                return map;
//            }
//        }).filter(new Function<Map<String, String>, Boolean>() {
//            @Override
//            public Boolean call(Map<String, String> v1) throws Exception {
//                return v1.get("null") != "null";
//            }
//        });
//
//        rdd.foreachPartition(new VoidFunction<Iterator>() {
//            @Override
//            public void call(Iterator iterator) throws Exception {
//                while (iterator.hasNext() && iterator != null) {
//                    System.out.println(iterator.next());
//                }
//            }
//        });



        sc.stop();
    }
}
