package cn.it.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class sparkReadWordTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparkReadWordTest")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\科瑞\\WorkaAndProblem\\98767中国移动.xls");


    }
}
