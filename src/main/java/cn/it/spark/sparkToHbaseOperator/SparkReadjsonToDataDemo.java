package cn.it.spark.sparkToHbaseOperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkReadjsonToDataDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SparkReadjsonToDataDemo")
                .setMaster("local[1]");

//        SQLContext sqlContext = new SQLContext(sc);
        SparkSession spark = SparkSession
                .builder()
                .appName("java spark sql base example")
                .config(conf)
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Dataset dataset = spark.read().json("C:\\Users\\Administrator\\Desktop\\a.txt");
        dataset.show();



    }
}
