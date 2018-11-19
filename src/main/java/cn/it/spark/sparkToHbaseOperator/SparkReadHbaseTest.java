package cn.it.spark.sparkToHbaseOperator;

import org.apache.avro.ipc.specific.Person;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkReadHbaseTest {

    public static void main(String[] args) throws IOException {

        SparkConf conf1 = new SparkConf()
                .setAppName("SparkReadHbaseTest")
                .set("spark.executor.memory", "1000m")
                .setMaster("local[2]")
                .set("spark.serlializer", "org.apache.spark.serializer.KryoSerializer")
                .setJars(new String[]{"/D:/hbase/build/libs/hbase.jar"});
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        conf1.set("spark.cores.max", "4");
        SparkSession spark = SparkSession
                .builder()
                .appName("java spark sql base example")
                .config(conf1)
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());


        try {
            String tableName = "scores";
            String[] familys = {"family1", "family2"};
            HbaseTest.craeteTable(tableName, familys);
            //add record row and row2
            HbaseTest.addRecord(tableName, "row1", "family1", "name", "张三");
            HbaseTest.addRecord(tableName, "row1", "fmily1", "age", "21");
            HbaseTest.addRecord(tableName, "row2", "family2", "q1", "97");
            HbaseTest.addRecord(tableName, "row2", "family2", "q2", "87");
            System.out.println("==============get one record============================");
            HbaseTest.getOneRecored(tableName, "row1");
            System.out.println("===============get all record=========================================");
            HbaseTest.getAllRecord(tableName);

            System.out.println("======================del one record ======================");
            HbaseTest.delRecord(tableName, "row2");

        } catch (IOException e) {
            e.printStackTrace();
        }

        Scan scan = new Scan();
        //  scan.setStartRow(Bytes.toBytes("195861-1035177490"));
        //  scan.setStopRow(Bytes.toBytes("195861-1072173147"));

        scan.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("family2"), Bytes.toBytes("q1"));

        List<Filter> filters = new ArrayList<>();

        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("family1"),
                Bytes.toBytes("q1"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("88"));
        filter.setFilterIfMissing(true);//if set true will skip row which column doesn't exist
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                Bytes.toBytes("family2"),
                Bytes.toBytes("q2"),
                CompareFilter.CompareOp.LESS,
                Bytes.toBytes("111"));
        filter2.setFilterIfMissing(true);

        PageFilter filter3 = new PageFilter(10);
        filters.add(filter);
        filters.add(filter2);
        filters.add(filter3);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);

        conf1.set(TableInputFormat.INPUT_TABLE, "scores");
        conf1.set(TableInputFormat.SCAN, HbaseTest.converScanToString(scan));

        //读取HBASE表数据 并创建RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(HbaseTest.conf,
                TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD<String> rdd = sc.textFile("hdfs://10.3.10.146:9000//");
        long count = hbaseRDD.count();
        System.out.println("count" + count);

        JavaRDD<Person> datas2 = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Person>() {
            @Override
            public Person call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                Result result = immutableBytesWritableResultTuple2._2();
                byte[] o = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("art"));
                if (o != null) {
                    Person person = new Person();
                    person.setName(Bytes.toString(result.getRow()));

                    return person;
                }

                return null;
            }
        });
        Dataset<Row> data = spark.createDataFrame(datas2, Person.class);
        data.show();


        //write data to hbase
        Job newAPIJobConfiguration1 = Job.getInstance(HbaseTest.conf);
        newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "scores");
        newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
        //create key , value pair to store in hbase
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = data.javaRDD().mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                Put put = new Put(Bytes.toBytes(row.<String>getAs("name") + "test"));
                put.add(Bytes.toBytes("family"), Bytes.toBytes("q1"),
                        Bytes.toBytes(String.valueOf(row.<Long>getAs("age"))));
                return new Tuple2<ImmutableBytesWritable, Put>(
                        new ImmutableBytesWritable(), put);
            }
        });
        //save to habse - spark built - in API method
        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
        spark.catalog();

    }
}
