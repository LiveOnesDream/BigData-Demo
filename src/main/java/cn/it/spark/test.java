//package cn.it.spark;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
//import org.apache.hadoop.hbase.util.Base64;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//
//import db.insert.HBaseDBDao;
//import scala.Tuple2;
//
//import java.io.IOException;
//import java.io.Serializable;
//import java.util.Arrays;
//import java.util.List;
//
//public class HBaseSparkQuery implements Serializable {
//
//    private static final long serialVersionUID = 1L;
//
//    public Log log = LogFactory.getLog(HBaseSparkQuery.class);
//
//    /**
//     * 将scan编码，该方法copy自 org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
//     *
//     * @param scan
//     * @return
//     * @throws IOException
//     */
//    static String convertScanToString(Scan scan) throws IOException {
//        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//        return Base64.encodeBytes(proto.toByteArray());
//    }
//
//    public void start() {
//        //初始化sparkContext，
//        SparkConf sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//
//        //使用HBaseConfiguration.create()生成Configuration
//        // 必须在项目classpath下放上hadoop以及hbase的配置文件。
//        Configuration conf = HBaseConfiguration.create();
//        //设置查询条件，这里值返回用户的等级
//        Scan scan = new Scan();
////        scan.setStartRow(Bytes.toBytes("57348556169_0000000000000"));
////        scan.setStopRow(Bytes.toBytes("57348556169_9999999999999"));
//        scan.addFamily(Bytes.toBytes("cids"));
//        scan.addFamily(Bytes.toBytes("times"));
//        scan.addFamily(Bytes.toBytes("pcis"));
//        scan.addFamily(Bytes.toBytes("angle"));
//        scan.addFamily(Bytes.toBytes("tas"));
//        scan.addFamily(Bytes.toBytes("gis"));
//        scan.addColumn(Bytes.toBytes("cids"), Bytes.toBytes("cid"));
//        scan.addColumn(Bytes.toBytes("times"), Bytes.toBytes("time"));
//        scan.addColumn(Bytes.toBytes("pcis"), Bytes.toBytes("pci"));
//        scan.addColumn(Bytes.toBytes("angle"), Bytes.toBytes("st"));
//        scan.addColumn(Bytes.toBytes("angle"), Bytes.toBytes("ed"));
//        scan.addColumn(Bytes.toBytes("tas"), Bytes.toBytes("ta"));
//        scan.addColumn(Bytes.toBytes("gis"), Bytes.toBytes("lat"));
//        scan.addColumn(Bytes.toBytes("gis"), Bytes.toBytes("lng"));
//        try {
//            //需要读取的hbase表名
//            String tableName = "mapCar";
//            conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            conf.set(TableInputFormat.SCAN, convertScanToString(scan));
//
//            //获得hbase查询结果Result
//            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf,
//                    TableInputFormat.class, ImmutableBytesWritable.class,
//                    Result.class);
//
//            //从result中取出用户年龄
//            JavaRDD<String> cars = hBaseRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>(){
//                private static final long serialVersionUID = 1L;
//
//                @Override
////                public Iterable<String>call(
//                        Tuple2<ImmutableBytesWritable, Result> t)
//                        throws Exception {
////                    String cid = Bytes.toString(t._2.getValue(Bytes.toBytes("cids"), Bytes.toBytes("cid")));
////                    String time = Bytes.toString(t._2.getValue(Bytes.toBytes("times"), Bytes.toBytes("time")));
////                    String pci = Bytes.toString(t._2.getValue(Bytes.toBytes("pcis"), Bytes.toBytes("pci")));
////                    String st = Bytes.toString(t._2.getValue(Bytes.toBytes("angle"), Bytes.toBytes("st")));
////                    String ed = Bytes.toString(t._2.getValue(Bytes.toBytes("angle"), Bytes.toBytes("ed")));
////                    String ta = Bytes.toString(t._2.getValue(Bytes.toBytes("tas"), Bytes.toBytes("ta")));
////                    String lat = Bytes.toString(t._2.getValue(Bytes.toBytes("gis"), Bytes.toBytes("lat")));
////                    String lng = Bytes.toString(t._2.getValue(Bytes.toBytes("gis"), Bytes.toBytes("lng")));
////                    return Arrays.asList("cid : "+cid+", time: "+time+", pci: "+pci+", st: "+st+", ed: "+ed+", ta: "+ta+", lat: "+lat+", lon: "+lng);
////                   return Arrays.asList(cid);
//                        return null;
//                }
//
//            });
////            JavaRDD<String>car = cars.distinct();
//            //打印出最终结果
//            cars.foreach(new VoidFunction<String>(){
//                private static final long serialVersionUID = 1L;
//
//                @Override
//                public void call(String s) throws Exception {
//                    System.out.println(s);
//                }
//            });
//            ;
//
////            //打印出最终结果
////            List<String> output = car.collect();
////            for (String s : output) {
////                System.out.println(s);
////            }
//
//        } catch (Exception e) {
//            log.warn(e);
//        }
//
//    }
//
//    /**
//     * spark如果计算没写在main里面,实现的类必须继承Serializable接口，<br>
//     * </>否则会报 Task not serializable: java.io.NotSerializableException 异常
//     */
//    public static void main(String[] args) throws InterruptedException {
//        new HBaseSparkQuery().start();
//    }
//}
