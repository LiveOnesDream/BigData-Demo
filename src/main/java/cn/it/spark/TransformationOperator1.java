package cn.it.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class TransformationOperator1 {
    public static final SparkConf conf = new SparkConf()
            .setMaster("local")
            .setAppName("TransformationOperator1")
            .set("spark.default.parallelism", "5");
    public static final JavaSparkContext sc = new JavaSparkContext(conf);

    public static void RepartitionOperator() {
        List<String> students = Arrays.asList("stu1", "stu2,", "stu3");
        JavaRDD<String> cls = sc.parallelize(students, 2);
        JavaRDD<String> rdd = cls.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>() {
                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> str) throws Exception {
                        List<String> list = new ArrayList<>();
                        while (str.hasNext()) {
                            String stu = str.next();
                            stu = "[" + index + "]" + stu;
                            list.add(stu);
                        }
                        return list.iterator();
                    }
                }, true);

        JavaRDD<String> rdd2 = rdd.repartition(3);//增加到三个分区

        JavaRDD<String> result = rdd2.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>() {
                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> str) throws Exception {
                        List<String> list = new ArrayList<>();
                        while (str.hasNext()) {
                            String stu = str.next();
                            stu = "[" + index + "]" + str;
                            list.add(stu);
                        }
                        return list.iterator();
                    }
                }, true);

        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String value) throws Exception {
                System.out.println(value);
            }
        });
    }

    public static void TransformationFlatMap() {
        List<String> list = Arrays.asList("hello ha", "nihao haha", "hello hao");
        JavaRDD<String> rdd = sc.parallelize(list);
        JavaRDD<String> result = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String value) throws Exception {

                return Arrays.asList(value.split(" ")).iterator();
            }
        });
        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    public static void CollectOperator() {
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(numberList);
        JavaRDD<Integer> temp = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) throws Exception {
                return num * 2;
            }
        });
        List<Integer> result = temp.collect();
        for (Integer num : result) {
            System.out.println(num);
        }
    }

    public static void CountOperator() {
        List<String> stu = Arrays.asList("stu1", "stu2", "stu3", "stu4", "stu5");
        JavaRDD<String> rdd = sc.parallelize(stu);
        long count = rdd.count();
        System.out.println("\n \n \n " + count);
    }

    public static void GroupByKeyOperator() {
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("zhangsan", 100),
                new Tuple2<>("zhangsan", 50),
                new Tuple2<>("lisi", 99),
                new Tuple2<>("wangwu", 120),
                new Tuple2<>("wangwu", 30)
        );

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list, 2);
        JavaPairRDD<String, Iterable<Integer>> result = rdd.groupByKey(3);

        result.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> score) throws Exception {
                System.out.println(score._1 + " " + score._2);
            }
        });

    }

    //传入两个个参数并返回一个结果
    public static void ReduceOperator() {
        List<Integer> num = Arrays.asList(11, 11, 11, 11, 11, 11);
        JavaRDD<Integer> rdd = sc.parallelize(num);

        int sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("\n\n\n\n\n" + sum);
    }

    /**
     * reduceByKey(func, [numTasks]) 算子示例
     * <p>
     * 简单的说就是groupByKey + reduce。先按照Key进行分组，然后将每组的Key进行reduce操作，得到一个Key对应一个Value的RDD。
     * 第二个参数就是指定使用多少task来执行reduce操作。
     */
    public static void ReduceByKeyOperator() {
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("zhangsan", 50),
                new Tuple2<String, Integer>("lisi", 99),
                new Tuple2<String, Integer>("wangwu", 120),
                new Tuple2<String, Integer>("wangwu", 30)
        );

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> num) throws Exception {
                System.out.println("name:" + num._1 + "age:" + num._2);
            }
        });
    }

    /**
     * sample(withReplacement, fraction, seed)
     * 对RDD中的数据进行随机取样操作，sample第一个参数代表产生的样本数据是否可以重复，
     * 第二个参数代表取样的比例，第三个数值代表一个随机数种子，如果传入一个常数，那么每次取样结果会一样。
     */
    public static void SampleOperator() {
        List<String> stu = Arrays.asList("stu1", "stu2", "stu3", "stu4", "stu5", "stu6");
        JavaRDD<String> rdd = sc.parallelize(stu);
        // 第一个参数决定取样结果是否可重复,第二个参数决定取多少比例的数据,第三个是自定义的随机数种子，如果传入一个常数则每次产生的值一样
        rdd.sample(false, 0.5).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    public static void TakeOperator() {
        List<Integer> number = Arrays.asList(1, 2, 3, 4, 5, 5, 6);
        JavaRDD<Integer> numRDD = sc.parallelize(number);

        List<Integer> nums = numRDD.take(3);
        for (Integer num : nums) {
            System.out.println("\n\n\n" + num);
        }
    }

    public static void TakeSampleOperator() {
        List<Integer> num = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> numRDD = sc.parallelize(num);

        List<Integer> nums = numRDD.takeSample(true, 2);
        for (Integer n : nums) {
            System.out.println("\n\n\n\n\n" + n);
        }
    }

    public static void UnionOperator() {
        List<String> name1 = Arrays.asList("stu1", "stu2", "stu3");
        List<String> name2 = Arrays.asList("stu1", "stu5", "stu6");

        JavaRDD<String> nRDD1 = sc.textFile("C:\\Users\\Administrator\\Desktop\\Wordcount.txt");
        JavaRDD<String> nRDD2 = sc.textFile("C:\\Users\\Administrator\\Desktop\\a.txt");
        JavaRDD<String> splited = nRDD1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String vallue) throws Exception {
                return Arrays.asList(vallue.split(" ")).iterator();
            }
        });
        splited.union(nRDD2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    public static void SaveAsTextFileOperator() {
        List<Integer> num = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(num);
        JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) throws Exception {
                return num * 2;
            }
        });
        result.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\SaveAsTextFileOperator.txt");
    }

    /**
     * Intersection 示例
     * 作用就是将两个RDD求交集，当然也进行了去重操作。
     */
    public static void IntersectionOperator() {
        List<String> str1 = Arrays.asList("stu1", "stu2", "stu2");
        List<String> str2 = Arrays.asList("stu2", "stu3", "stu3");

        JavaRDD<String> rdd1 = sc.parallelize(str1, 1);
        JavaRDD<String> rdd2 = sc.parallelize(str2, 1);

        rdd1.intersection(rdd2).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\IntersectionOperator.txt");

    }

    /**
     * cartesian(otherDataset)
     * 相当于进行了一次笛卡尔积的计算，将两个RDD中的数据一一对应起来
     */
    public static void CartesianOperator() {
        List<String> hero = Arrays.asList("张飞", "貂蝉", "吕布");
        List<String> skill = Arrays.asList("闪现", "斩杀", "眩晕");

        JavaRDD<String> rdd1 = sc.parallelize(hero, 2);
        JavaRDD<String> rdd2 = sc.parallelize(skill, 2);

        rdd1.cartesian(rdd2).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\Cartesian.txt");
    }

    /**
     * CountByKey 示例
     * 只能用在(K,V)类型，用来统计每个key的数据有多少个，返回一个(K,Int)。
     */
    public static void CountByKeyOperator() {
        List<Tuple2<String, String>> stus = Arrays.asList(
                new Tuple2<String, String>("class1", "stu1"),
                new Tuple2<String, String>("class1", "stu2"),
                new Tuple2<String, String>("class2", "stu3"),
                new Tuple2<String, String>("class1", "stu4"));
        JavaPairRDD<String, String> pairsRDD = sc.parallelizePairs(stus);
        Map<String, Long> map = pairsRDD.countByKey();
        for (String key : map.keySet()) {
            Long value = map.get(key);
            System.out.println("key:\t" + key + "\tvalue\t:" + value);
        }

    }

    /**
     * cogroup(otherDataset, [numTasks])。
     * 将两个RDD按照Key进行汇总，
     * 第一个RDD中的Key对应的数据放在一个Iterable中，
     * 第二个RDD中同样的Key对应的数据放在一个Iterable中，
     * 最后得到一个Key，对应两个Iterable的数据。
     * 第二个参数就是指定task数量
     */
    public static void CogroupOperator() {
        List<Tuple2<String, String>> stus = Arrays.asList(
                new Tuple2<String, String>("stu1", "zhangsan"),
                new Tuple2<String, String>("stu2", "lisi"),
                new Tuple2<String, String>("stu3", "lisi"),
                new Tuple2<String, String>("stu2", "wangwu"),
                new Tuple2<String, String>("stu2", "lisi")
        );
        List<Tuple2<String, String>> scores = Arrays.asList(
                new Tuple2<String, String>("stu1", "90"),
                new Tuple2<String, String>("stu1", "100"),
                new Tuple2<String, String>("stu2", "80"),
                new Tuple2<String, String>("stu3", "120")
        );
        JavaPairRDD<String,String> stuRDD = sc.parallelizePairs(stus);
        JavaPairRDD<String,String> scoreRDD = sc.parallelizePairs(scores);

        JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> result = stuRDD.cogroup(scoreRDD);
        result.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> result) throws Exception {
                System.out.println(result._1);
                System.out.println(result._2._1);
                System.out.println(result._2._2);
                System.out.println();
            }
        });
        result.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\cogroup.txt");
    }
}
