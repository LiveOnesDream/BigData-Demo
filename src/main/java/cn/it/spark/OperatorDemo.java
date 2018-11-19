package cn.it.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

public class OperatorDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("mapDemo")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> line = sc.textFile("C:\\Users\\Administrator\\Desktop\\Wordcount.txt");

        JavaRDD<String> map1 = line.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                String replace = v1.replaceAll(" ", "@");

                return replace;
            }
        });
        JavaRDD<String> flatMap1 = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split("  ")).iterator();
            }
        });
        flatMap1.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
//        map1.foreach(new VoidFunction<String>(){
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
    }
}
