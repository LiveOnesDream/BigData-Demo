package cn.it.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.ArrayList;
import java.util.List;

public class RDD2DataFrameProgrammatically {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
//                .set("spark.sql.warehouse.dir",)
                .setAppName("RDD2DataFrameProgrammatically")
                .setMaster("local[3]");
        SparkSession sparkSession = SparkSession
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
        Dataset<Person> javaBeanDS = sparkSession.createDataset(
                personpList,
                personEncoder
        );
        javaBeanDS.show();

        JavaRDD<student> rdd = sparkSession.read()
                .textFile("C:\\Users\\Administrator\\Desktop\\students.txt")
                .javaRDD()
                .map(new Function<String, student>() {
                    @Override
                    public student call(String str) throws Exception {
                        String[] splited = str.split(" ,");
                        student stu = new student();
                        stu.setId(Integer.valueOf(splited[0]));
                        stu.setName(String.valueOf(splited[1]));
                        stu.setAge(Integer.valueOf(splited[2]));
                        return stu;
                    }
                });
        //表结构推断
        Dataset<Row> stuDF = sparkSession.createDataFrame(rdd, student.class);
        stuDF.createOrReplaceTempView("student");

        //定义map 这里对每个元素做序列化操作
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> studentDF = stuDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                return "Name:" + value.getString(1) + "and age is " + String.valueOf(value.getInt(0));
            }
        }, stringEncoder);
        studentDF.show();
        JavaRDD<String> rowrdd = sparkSession.sparkContext()
                .textFile("../", 1).toJavaRDD();

        // 创建一个描述表结构的schema
        String schemaString = "id name age ";
        List<StructField> structFields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            structFields.add(field);
        }

        StructType schema = DataTypes.createStructType(structFields);
        JavaRDD<Row> rowRDD = rowrdd.map(new Function<String, Row>() {
            @Override
            public Row call(String row) throws Exception {
                String[] splited = row.split(" ");
                return RowFactory.create(splited[0], splited[1].trim());
            }
        });
        // Apply the schema to the RDD
        Dataset<Row> stuDataFrame = sparkSession.createDataFrame(rowRDD, schema);
        stuDataFrame.createOrReplaceTempView("students");
        stuDataFrame.show();


//        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
//        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
//        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

//        StructType structType = DataTypes.createStructType(structFields);
//        Dataset<Row> studentDS = sparkSession.createDataFrame(rdd, student.class);
//        studentDS.registerTempTable("students");
//        Dataset teenagerDS = sparkSession.sql("select * from student from student where age >18");
//        List<Row> rows = teenagerDS.javaRDD().collect();
//        for (Row row : rows) {
//            System.out.println(row);
//        }
    }
}
