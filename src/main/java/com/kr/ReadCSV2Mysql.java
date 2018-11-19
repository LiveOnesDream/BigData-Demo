package com.kr;

import com.StatisticAnalyze_MySQL.DataInit;
import com.databricks.spark.csv.util.TextFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 读取csv文件两种方式
 */

public class ReadCSV2Mysql {

    static final Logger log = Logger.getLogger(ReadCSV2Mysql.class);

    public static SparkConf Sconf = null;
    public static SparkContext sc = null;
    public static SQLContext sqlContext = null;
    public static SparkSession sparkSession = null;
    public static Properties properties = new Properties();
    public static String ResultURL = "jdbc:mysql://localhost:3306/jdyp_depot?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

    static {
        Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadCSV_v2")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "3");
        sc = new SparkContext(Sconf);
        sqlContext = new SQLContext(sc);
        sparkSession = SparkSession.builder().config(Sconf).getOrCreate();

        properties.put("user", "root");
        properties.put("password", "");
        properties.put("driver", "com.mysql.jdbc.Driver");

    }

    public static String filePath = "C:\\Users\\Administrator\\Desktop\\test\\13385644444张国定.xls";
    public static String charset = "utf-8";

    public static void main(String[] args) throws IOException {

        ArrayList<File> fileList = getFiles("F:\\csv");
        for (File file : fileList) {
            getCSVData1(file.getPath());
        }

        sc.stop();

    }

    public static void getCSVData() throws IOException {

        RDD<String> inputRDD = TextFile.withCharset(sc, filePath, charset);
        String header = inputRDD.first();

//        JavaRDD<String> filterFirst = inputRDD.toJavaRDD().filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String v1) throws Exception {
//                return v1 != "null";
//            }
//        });
//
//        JavaRDD<Row> valueRDD =  filterFirst.map(new Function<String, Row>() {
//            @Override
//            public Row call(String value) throws Exception {
//                List<String> rowList = new ArrayList<>();
//                String[] splits = value.split(",");
//                for (int i = 0; i < splits.length; i++) {
//                    rowList.add(splits[i]);
//
//                }
//                return RowFactory.create(rowList.toArray());
//            }
//        });
        //获取csv的字段 schema信息
        CSVRecord csvRecord = CSVParser.parse(header, CSVFormat.DEFAULT).getRecords().get(0);
        List<String> csvSchemaColumns = new ArrayList<>();
        Iterator<String> iterator = csvRecord.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            csvSchemaColumns.add(i, iterator.next());
        }
//            String fileds = iterator.next();
//            if (fileds.equals("记录类型1")) {
//                fileds = "record_type_one";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("记录类型")) {
//                fileds = "record_type_two";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("呼叫日期")) {
//                fileds = "call_date";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("呼叫时间")) {
//                fileds = "call_time";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("呼叫时长")) {
//                fileds = "call_duration";
//                csvSchemaColumns.add(fileds);
//            }
//
//            if (fileds.equals("事件类型1")) {
//                fileds = "event_type_one";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("事件类型")) {
//                fileds = "event_type_two";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("用户号码")) {
//                fileds = "user_number";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("用户归属地")) {
//                fileds = "user_affiliation";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("IMSI")) {
//                fileds = "imsi";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("IMEI")) {
//                fileds = "imei";
//                csvSchemaColumns.add(fileds);
//            }
//
//            if (fileds.equals("对方号码")) {
//                fileds = "other_number";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("对方归属地")) {
//                fileds = "other_affiliation";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("LAI")) {
//                fileds = "lai";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("CI")) {
//                fileds = "ci";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("基站地址1")) {
//                fileds = "station_address_one";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("基站地址")) {
//                fileds = "station_address_two";
//                csvSchemaColumns.add(fileds);
//            }
//            if (fileds.equals("通话所在地区号")) {
//                fileds = "call_address";
//                csvSchemaColumns.add(fileds);
//            }


        //获取数据
        JavaRDD<Row> dataRDD = inputRDD.toJavaRDD().map(new Function<String, Row>() {
            @Override
            public Row call(String value) throws Exception {
                List<String> rowList = new ArrayList<>();
                String[] splits = value.split(",");
                for (int i = 0; i < csvSchemaColumns.size(); i++) {
                    if (!csvSchemaColumns.get(i).equals(splits[i])) {
                        rowList.add(splits[i]);
                    } else {
                        rowList.add(" ");
                    }
                }
                return RowFactory.create(rowList.toArray());
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v) throws Exception {
                return v.toString() != " ";
            }
        });

//        dataRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
//            @Override
//            public void call(Iterator<Row> ite) throws Exception {
//                while (ite.hasNext()) {
//                    Row row = ite.next();
//                    System.out.println(row.toString());
//                }
//            }
//        });

        List<StructField> structFields = new ArrayList<>();
        for (String columnStr : csvSchemaColumns) {
            structFields.add(DataTypes.createStructField(columnStr, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(structFields);
        Dataset csvTable = sparkSession.createDataFrame(dataRDD, schema);

        csvTable.createOrReplaceTempView("a");
        Dataset<Row> result = csvTable.sparkSession().sql("select * from a  ");

        JavaRDD<String> rdd = result.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return v1.toString();
            }
        });
        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> ite) throws Exception {
                while (ite.hasNext()) {
                    System.out.println(ite.next());
                }
            }
        });
//        csvTable.write().mode(SaveMode.Append).jdbc(DataInit.ResultURL,"bill_test",DataInit.properties);


    }

    public static void getCSVData1(String path ) {

            Dataset<Row> csvDT = sparkSession.read().format("com.databricks.spark.csv").
                    option("header", true).
                    option("nullValue", "").
                    option("delimiter", ",").
                    load(path).toDF("a", "b", "c", "d", "e", "f", "s", "q", "w", "r", "t", "y", "dd", "sss", "asd", "qwe", "FDSGDG", "DGFFDG");

//            csvDT.createOrReplaceTempView("test");
//            Dataset result = sparkSession.sql("select * from test");
            csvDT.write().mode(SaveMode.Overwrite).jdbc(ResultURL, "sparkSQL_bill_test", properties);

    }

    public static void getCSVData2() throws IOException {
        //读取csv文件  设置编码
        RDD<String> csvRDD = TextFile.withCharset(sc, filePath, charset);
        //文件字段
        String header = csvRDD.first();
        //获取文件schame 放到list集合中
        CSVRecord csvRecord = CSVParser.parse(header, CSVFormat.DEFAULT).getRecords().get(0);
        List<String> csvSchemaColumns = new ArrayList<>();
        Iterator<String> iterator = csvRecord.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            csvSchemaColumns.add(i, iterator.next());

        }
        //获取数据
        JavaRDD<Row> dataRDD = csvRDD.toJavaRDD().map(new Function<String, Row>() {
            @Override
            public Row call(String data) throws Exception {
                List<String> dataList = new ArrayList<>();
                String[] splited = data.split(",");
                for (int i = 0; i < splited.length; i++) {
                    if (!csvSchemaColumns.get(i).equals(splited[i])) {
                        dataList.add(splited[i]);
                    } else {
                        dataList.add(" ");
                    }

                }
                return RowFactory.create(dataList.toArray());
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v) throws Exception {
                return v.toString() != " ";
            }
        });


        dataRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> ite) throws Exception {
                for (int i = 0; ite.hasNext(); i++) {

                    System.out.println(ite.next());

                }
            }
        });


    }

    public static ArrayList<File> getFiles(String path) {
        ArrayList<File> files = new ArrayList<>();
        File file = new File(path);
        File[] tempList = file.listFiles();

        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
//              System.out.println("文件：" + tempList[i]);
                files.add(new File(tempList[i].toString()));
            }
            if (tempList[i].isDirectory()) {
//              System.out.println("文件夹：" + tempList[i]);
            }
        }
        return files;
    }
}
