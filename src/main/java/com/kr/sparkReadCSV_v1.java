package com.kr;

import com.databricks.spark.csv.util.TextFile;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;


public class sparkReadCSV_v1 {

    static final Logger log = Logger.getLogger(sparkReadCSV_v1.class);

//    public static String filePath = "C:\\Users\\Administrator\\Desktop\\test\\13470849876姚远.xls";
    public static String charset = "gbk";

    public static void main(String[] args) throws Exception {

        SparkConf Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "3");

//        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        SparkContext sc = new SparkContext(Sconf);
//        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sc);

//        String path = ExcelConvertCSV.exceltoCSV(filePath);
        String path = "C:\\Users\\Administrator\\Desktop\\科瑞\\账单2\\涉毒交易明细1.csv";

        RDD<String> inputRDD = TextFile.withCharset(sc, path, charset);

        String header = inputRDD.first();

        //获取csv的字段 schema信息
        CSVRecord csvRecord = CSVParser.parse(header, CSVFormat.DEFAULT).getRecords().get(0);
        List<String> csvSchemaColumns = new ArrayList<>();

        Iterator<String> iterator = csvRecord.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            csvSchemaColumns.add(i, iterator.next());
        }

        for (String str : csvSchemaColumns) {
            System.out.println(str);
        }
        //按行读取csv文件数据，生成key，value对
        JavaRDD rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String, String>>() {
            @Override
            public Map<String, String> call(String v1) throws Exception {
                Map<String, String> map = new HashMap<>();
                String[] fields = v1.split(",");
                for (int i = 0; i < fields.length; i++) {
                    if (!csvSchemaColumns.get(i).equals(fields[i])) {
                        map.put(csvSchemaColumns.get(i), fields[i]);
                    } else {
                        map.put("null", "null");
                    }
                }
                return map;
            }
        }).filter(new Function<Map<String, String>, Boolean>() {
            @Override
            public Boolean call(Map<String, String> v1) throws Exception {
                return v1.get("null") != "null";
            }
        });

        rdd.foreachPartition(new VoidFunction<Iterator>() {
            @Override
            public void call(Iterator iterator) throws Exception {
                int count = 0;
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                    count ++;
                }
                System.out.println(count);
            }
        });


//        line.map(new Function<String, Object>() {
//            @Override
//            public Object call(String v1) throws Exception {
//                return v1;
//            }
//        }).foreachPartition(new VoidFunction<Iterator<Object>>() {
//            @Override
//            public void call(Iterator<Object> v) throws Exception {
//                while (v.hasNext()){
//                    String value = (String) v.next();
//                   log.info(value);
//                }
//            }
//        });
        sc.stop();
    }
}
