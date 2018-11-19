package cn.it.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class spark_Test {
    public static void main(String[] args) {


        String warehouseLocation = System.getProperty("D://workSpace//BigData-Demo") + "spark-warehouse";//用户的当前工作目录
        SparkConf conf = new SparkConf().setAppName("spark sql test")
                .set("spark.sql.warehouse.dir", warehouseLocation)
                .setMaster("local[3]");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //为自定义的对象创建Dataset
        List<Person> personpList = new ArrayList<Person>();
        Person person1 = new Person();
        person1.setName("Andy");
        person1.setAge(32);
        Person person2 = new Person();
        person2.setName("Justin");
        person2.setAge(19);
        personpList.add(person1);
        personpList.add(person2);
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                personpList,
                personEncoder
        );
        javaBeanDS.show();
        //spark支持使用java 反射机制推断表结构
        //1 首先创建一个存储person对象的RDD
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("C:\\Users\\Administrator\\Desktop\\students.txt")
                .javaRDD()
                .map(new Function<String, Person>() {
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    }
                });
        //2 表结构推断
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");

        //3 定义map 这里对每个元素做序列化操作
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> peopleSerDF = peopleDF.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(1) + " and age is " + String.valueOf(row.getInt(0));
            }
        }, stringEncoder);
        peopleSerDF.show();
        //==============================================3 从RDD创建Dataset StructType对象的使用
        JavaRDD<String> peopleRDD2 = spark.sparkContext()
                .textFile("..\\sparkTestData\\people.txt", 1)
                .toJavaRDD();

        // 创建一个描述表结构的schema
        String schemaString = "name age";
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD2.map(new Function<String, Row>() {
            //@Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");
        peopleDataFrame.show();

    }


}
