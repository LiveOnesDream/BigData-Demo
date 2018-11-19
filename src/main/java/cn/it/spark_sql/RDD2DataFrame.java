package cn.it.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class RDD2DataFrame {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrame")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\student.txt");
        JavaRDD<student> students = lines.map(new Function<String, student>() {
            @Override
            public student call(String lines) throws Exception {
                String[] linessplitedString = lines.split(" ");
                student stu = new student();
                stu.setId(Integer.valueOf(linessplitedString[0]));
                stu.setName(linessplitedString[1]);
                stu.setAge(Integer.valueOf(linessplitedString[2]));

                return stu;
            }
        });
        Dataset studentDS = sqlContext.createDataFrame(students,student.class);
        studentDS.registerTempTable("students");
        Dataset teenagerDS=sqlContext.sql("select * from students where age <= 18");
        JavaRDD<Row> teenagerRDD = teenagerDS.javaRDD();

        JavaRDD<student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, student>() {
            @Override
            public student call(Row row) throws Exception {
                student stu = new student();
                stu.setAge(row.getInt(0));
                stu.setId(row.getInt(1));
                stu.setName(row.getString(2));
                return stu;
            }
        });
        List<student> studentList = teenagerStudentRDD.collect();
        for (student stu : studentList){
            System.out.println(stu);
        }
    }
}
