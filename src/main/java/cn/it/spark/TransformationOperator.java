package cn.it.spark;

import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import scala.Array;
import scala.Tuple1;
import scala.Tuple2;

import java.util.*;

public class TransformationOperator {

    public static SparkConf conf = new SparkConf()
            .setAppName("test")
            .setMaster("local");
    public static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {
//        TransformationMap();
//        TransformationMap1();
//        TransformationMpPartitions();
//        mapPartitionsWithIndex();
//        TransformationFilter1();
        TransformationCoalesce();  // 2[0],1[0]stu1
//                                    2[0],1[1]stu2
//        TransformationFlatmap();
//        TransformationFilter();
//        TransformationgroupByKey();
//        TransformationgroupByKey2();
    }

    private static void TransformationCoalesce() {
        List<String> list =Arrays.asList("stu1","stu2","stu3","stu4");
//        2[0],1[0]stu1
//        2[0],1[1]stu2
        JavaRDD<String> cls = sc.parallelize(list,4);
        JavaRDD<String> temp = cls.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> cls) throws Exception {
                List<String> list = new ArrayList<>();
                while (cls.hasNext()){
                    String stu = cls.next();
                    stu = "["+index+"]" +stu;
                    list.add(stu);
                }
                return list.iterator();
            }
        },true);
        temp.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.print(s );
            }
        });
        JavaRDD<String> temp2 = temp.coalesce(2);
        JavaRDD<String> result = temp2.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> str) throws Exception {
                List<String> list = new ArrayList<>();
                while (str.hasNext()){
                    String  stu = str.next();
                    stu = "["+index+"],"+stu;
                    list.add(stu);
                }
                return list.iterator();
            }
        },true);
//        result.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
    }

    private static void TransformationFilter1() {
        final List<Integer> scores  = Arrays.asList(21,34,5,6,7,90);
        JavaRDD<Integer> rdd = sc.parallelize(scores);

        JavaRDD<Integer> result = rdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer num) throws Exception {
                return num < 60;
            }
        });
        result.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    private static void mapPartitionsWithIndex() {
        final List list = Arrays.asList("张三", "李四", "王五");
        JavaRDD<String> rdd = sc.parallelize(list);

        JavaRDD<String> result = rdd.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>() {

                    @Override
                    public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                        List<String> list = new ArrayList<>();
                        while (v2.hasNext()) {
                            String name = v2.next();
                            name = v2 + " " + v1;
                            list.add(name);
                        }

                        return list.iterator();
                    }
                }, true);
        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s
                );
            }
        });
    }


    private static void TransformationMpPartitions() {
        List<String> names = Arrays.asList("张三", "李四", "王五");
        JavaRDD<String> nameRdd = sc.parallelize(names);

        final Map<String, Integer> scoreMap = new HashMap<>();
        scoreMap.put("张三", 100);
        scoreMap.put("李四", 99);
        scoreMap.put("王五", 98);

        JavaRDD<Integer> scoreRDD = nameRdd.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<String> stringIterator) throws Exception {
                List<Integer> scores = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String name = stringIterator.next();
                    int score = scoreMap.get(name);
                    scores.add(score);
                }
                return scores.iterator();
            }
        });
        scoreRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    private static void TransformationMap1() {
        final List list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<String> num = rdd.map(new Function<Integer, String>() {
            @Override
            public String call(Integer v1) throws Exception {
                return "numbers" + v1;
            }
        });
        num.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }


    public static void TransformationMap() {
        final List list = Arrays.asList("张三 ", " 李四  ", " 王五");
        JavaRDD<String> rdd = sc.parallelize(list);
        rdd.map(new Function<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "hello" + name;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }

    public static void TransformationFlatmap() {
        final List list = Arrays.asList("张三 李四", "李思 张三", "王五 赵柳");
        final JavaRDD rdd = sc.parallelize(list);
        rdd.flatMap(new FlatMapFunction() {
            @Override
            public Iterator call(Object o) throws Exception {
                return Arrays.asList(String.valueOf(o).split(" ")).iterator();
            }
        }).map(new Function() {
            @Override
            public Object call(Object v1) throws Exception {
                return "hello" + v1;
            }
        }).foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }

    public static void TransformationFilter() {
        final List list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD rdd = sc.parallelize(list);
        JavaRDD filterRDD = rdd.filter(new Function() {
            @Override
            public Object call(Object v1) throws Exception {
                return Integer.valueOf(String.valueOf(v1)) % 2 == 0;
            }
        });
        filterRDD.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }

    private static void TransformationgroupByKey() {
        /**
         * RDD()
         * bykey
         * -结果：
         * 峨眉
         * 周芷若 灭绝师太
         * 武当
         * 宋青书 张三丰
         *
         *  new Tuple2<>("峨眉", "周芷若"),
         *                 new Tuple2<>("峨眉", "灭绝师太"),
         *                 new Tuple2<>("武当", "宋青书"),
         *                 new Tuple2<>("武当", "张三丰")
         */
        final List<Tuple2> list = Arrays.asList(
                new Tuple2<>("峨眉", "周芷若"),
                new Tuple2<>("峨眉", "灭绝师太"),
                new Tuple2<>("武当", "宋青书"),
                new Tuple2<>("武当", "张三丰")
        );
//        JavaPairRDD<String,Tuple2> rdd =

    }

    public static void TransformationgroupByKey2() {

        List<Integer> data = Arrays.asList(1, 1, 2, 2, 1);
        JavaRDD<Integer> distData = sc.parallelize(data);

        JavaPairRDD<Integer, Integer> firstRDD = distData.mapToPair(
                new PairFunction<Integer, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                        return new Tuple2(integer, integer * integer);
                    }
                });

        JavaPairRDD<Integer, Iterable<Integer>> secondRDD = firstRDD.groupByKey();

        List<Tuple2<Integer, String>> reslist = secondRDD.map(
                new Function<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                        int key = integerIterableTuple2._1();
                        StringBuffer sb = new StringBuffer();
                        Iterable<Integer> iter = integerIterableTuple2._2();
                        for (Integer integer : iter) {
                            sb.append(integer).append(" ");
                        }
                        return new Tuple2(key, sb.toString().trim());
                    }
                }).collect();


        for (Tuple2<Integer, String> str : reslist) {
            System.out.println(str._1() + "\t" + str._2());
        }
        sc.stop();

    }
}
