package cn.it.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AvgAgeCalculator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AvgAgeCalculator")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取文件
        JavaRDD<String> dataFile = sc.textFile("C:\\Users\\Administrator\\Desktop\\DataFile.txt");
        //对数据进行分片
        JavaRDD<String> agerdd = dataFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")[1]).iterator();
            }
        });
        //求出所有个数
        long count = agerdd.count();
        //类型转换
        JavaRDD<Integer> conAge = agerdd.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return Integer.valueOf(String.valueOf(s));
            }
        });
        Integer totalAge  = conAge.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        Double avg =totalAge .doubleValue()/count;
        System.out.println("Total Age:" + totalAge + ";    Number of People:" + count );
        System.out.println("Average Age is " + avg);
    }

}
