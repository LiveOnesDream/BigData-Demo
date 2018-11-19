package cn.it.spark.sparkToHbaseOperator;


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.derby.impl.sql.catalog.SYSCOLUMNSRowFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDateToBooleanViaLongToLong;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.jdbc.JdbcType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;
import org.jboss.netty.handler.ipfilter.CIDR;
import scala.Tuple2;
import scala.sys.Prop;
import sun.security.util.Length;

import javax.xml.bind.Element;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.sql.Connection;
import java.sql.Date;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;

public class sparkReadHbaseData {

    public static void main(String[] args) throws IOException {
        long starttime = System.currentTimeMillis();
        SparkConf Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
//        spark.serializer   org.apache.spark.serializer.KryoSerializer
                .setMaster("local[3]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "2");
        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        //HBASE连接  配置zookeeper  ....
        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        hconf.set("hbase.zookeeper.quorum", "192.168.65.154");
        hconf.set("hbase.master", "192.168.65.154:16030");

        String tableName = "Ticket_data1";
        //设置查询的表名，开始rowkey，结束rowkey，列族
        Scan scan = new Scan();
        hconf.set(TableInputFormat.INPUT_TABLE, tableName);
        hconf.set(TableInputFormat.SCAN, converScanToString(scan));
        //conf1.set(TableInputFormat.SCAN_ROW_STOP, "2017-04-2716:03:4335390");
        hconf.set(TableInputFormat.SCAN_CACHEBLOCKS, "cf");
        hconf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000);

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        //列RDD
        JavaRDD<List<String>> recordColumnRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, List<String>>() {
            @Override
            public List<String> call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                List<String> recordColumnList = new ArrayList<>();
                Result result = v1._2;
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    recordColumnList.add(new String(CellUtil.cloneQualifier(cell)));
                }
                return recordColumnList;

            }
        });
        //  dataRDD
        JavaRDD<Row> recordValueRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                List<String> recordList = new ArrayList<>();
                Result result = v1._2;
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    recordList.add(new String(CellUtil.cloneValue(cell)));
                }
                return RowFactory.create(recordList.toArray());
            }
        });

        // 设置字段
        List<StructField> structFields = new ArrayList<>();
        List<String> fieldList = recordColumnRDD.first();
        for (String columnStr : fieldList) {
            structFields.add(DataTypes.createStructField(columnStr, DataTypes.StringType, true));
        }
        //新建列schema
        StructType schema = DataTypes.createStructType(structFields);
        Dataset emp = sparkSession.createDataFrame(recordValueRDD, schema);
        emp.createOrReplaceTempView("Ticket_data1");
        Dataset sqlemp = sparkSession.sql("select * from Ticket_data1 where record_type_two != 'GPRS' ");

//        Row[] rows = (Row[]) emp.collect();
//        for (Row row : rows) {
//            System.out.println(row);
//        }

//        emp.printSchema();
//        emp.createOrReplaceTempView("Ticket");
//
//        Dataset<Row> sqlDT = sparkSession.sql("select *  from Ticket ");
//        sqlDT.show();
//        Properties properties = new Properties();
//        properties.setProperty("user", "jdyp");
//        properties.setProperty("password", "keyway123");
//        sqlDT.write().mode(SaveMode.Append).option("driver", "com.mysql.jdbc.Driver")
//                .jdbc("jdbc:mysql://192.168.13.204:3306/jdyp_depot?useUnicode=true&characterEncoding=utf8",
//                        " s_tel_box", properties);
//        sqlDT.toJavaRDD().saveAsTextFile("hdfs://10.3.10.134:8020/input/sparkToHDFS");


        sqlemp.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> row) throws Exception {
                Connection conn = null;
                PreparedStatement stmt = null;
                PreparedStatement stmt1 = null;
                Row row1 = null;

                String sql = "insert into s_ticket_details_copy1  (ticket_details_id,ticket_id," +
                        "record_type_one,record_type_two,call_date,call_time,event_type_one," +
                        "event_type_two,user_number,user_affiliation,imei,imsi,other_number," +
                        "other_affiliation,lai,ci,station_address_one,station_address_two," +
                        "call_address,create_user,create_time ) value (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                Properties properties = new Properties();
//                properties.setProperty("user", "root");
//                properties.setProperty("password", "");192.168.13.204
                properties.setProperty("user", "jdyp");
                properties.setProperty("password", "keyway123");

                String url = "jdbc:mysql://192.168.13.204:3306/jdyp_depot?useSSL=false&useUnicode=true&characterEncoding=utf8";
                try {

                    conn = DriverManager.getConnection(url, properties);
                    conn.setAutoCommit(false);
                    stmt = conn.prepareStatement(sql);

                    int count = 0;
                    long sum = 0;//统计插入的条数

                    long date = System.currentTimeMillis();
                    String create_time = String.valueOf(date / 1000);
                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    String date_str = ""; //日期字段+时间字段拼接
                    long endTim;

                    String user_number = "";
                    String user_affiliation = "";
                    String imei = "";
                    String imsi = "";
                    String other_number = "";
                    String other_affiliation = "";


                    while (row.hasNext()) {
                        count++;
                        row1 = row.next();
                        date_str = String.valueOf(row1.getAs("call_date").toString().trim() +
                                " " + row1.getAs("call_time").toString().trim());
                        stmt.setString(1, UUID.randomUUID().toString().replace("-", "").toUpperCase());
                        stmt.setString(2, String.valueOf(" "));
                        stmt.setString(3, row1.getAs("record_type_one"));
                        stmt.setString(4, row1.getAs("record_type_two"));
                        stmt.setTimestamp(5, Timestamp.valueOf(date_str));

                        if (StringUtils.isNotEmpty(row1.getAs("call_duration"))) {
                            stmt.setLong(6, Long.parseLong(row1.getAs("call_duration")));
                        } else {
                            stmt.setLong(6, 0L);
                        }

                        stmt.setString(7, row1.getAs("event_type_one"));
                        stmt.setString(8, row1.getAs("event_type_two"));

                        user_number = row1.getAs("user_number").toString();
                        user_affiliation = row1.getAs("user_affiliation").toString();
                        imei = row1.getAs("imei").toString();
                        imsi = row1.getAs("imsi").toString();
                        other_number = row1.getAs("other_number").toString();
                        other_affiliation = row1.getAs("other_affiliation").toString();

                        if (StringUtils.isNotEmpty(user_number)) {
                            stmt.setLong(9, Long.parseLong(user_number));
                        } else {
                            stmt.setLong(9, 0L);
                        }

                        if (StringUtils.isNotEmpty(user_affiliation)) {
                            stmt.setLong(10, Long.parseLong(user_affiliation));
                        } else {
                            stmt.setLong(10, 0L);
                        }

                        if (StringUtils.isNotEmpty(imei)) {
                            stmt.setLong(11, Long.parseLong(imei));
                        } else {
                            stmt.setLong(11, 0L);
                        }

                        if (StringUtils.isNotEmpty(imsi)) {
                            stmt.setLong(12, Long.parseLong(imsi));
                        } else {
                            stmt.setLong(12, 0L);
                        }

                        if (StringUtils.isNotEmpty(other_number) && other_number.length() <= 16) {
                            stmt.setLong(13, Long.parseLong(other_number));
                        } else {
                            stmt.setLong(13, 0L);
                        }
                        if (StringUtils.isNotEmpty(other_affiliation)) {
                            stmt.setLong(14, Long.parseLong(other_affiliation));
                        } else {
                            stmt.setLong(14, 0L);
                        }

                        stmt.setString(15, row1.getAs("lai"));
                        stmt.setString(16, row1.getAs("ci"));
                        stmt.setString(17, row1.getAs("station_address_one"));
                        stmt.setString(18, row1.getAs("station_address_two"));
                        stmt.setString(19, row1.getAs("call_address"));
                        stmt.setString(20, "");
                        stmt.setString(21, "");

                        stmt.addBatch();

                        System.out.println("插入数据：" + count + "条");
                        if (count % 5000 == 0) {
                            System.out.println("-------------------");
                            stmt.executeBatch();
                            conn.commit();
                            stmt.clearParameters();
                            System.out.println("条数  =  " + count + "执行" + "提交");
                        }
                        sum++;

                    }
                    stmt.executeBatch();
                    conn.commit();
                    System.out.println("插入总条数：" + sum);

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (conn != null) {
                        conn.close();
                    }
                    if (stmt != null) {
                        stmt.close();
                    }
                }
            }

        });

        sc.close();
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - starttime) / 1000 + "ms");


//        Scan scan = new Scan();
//        scan.addColumn("cf".getBytes(),"呼叫日期".getBytes());
//        scan.addColumn("cf".getBytes(),"用户归属地".getBytes());
//        List<Filter> filters = new ArrayList<>();
//
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"),
//                Bytes.toBytes("呼叫时间"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("2017-04-27"));
//        filter.setFilterIfMissing(true);//if set true will skip row which column doesn't exist
//        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("cf"),
//                Bytes.toBytes("用户归属地"), CompareFilter.CompareOp.LESS, Bytes.toBytes("0564"));
//        filter2.setFilterIfMissing(true);
//
//        PageFilter filter3 = new PageFilter(10);
//        filters.add(filter);
//        filters.add(filter2);
//        filters.add(filter3);
//
//        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
//        scan.setFilter(filterList);
//
//        conf.set(TableInputFormat.INPUT_TABLE, "Ticket_Data");
//        conf.set(TableInputFormat.SCAN, converScanToString(scan));
//
//        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(HbaseTest.conf,
//                TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//        JavaRDD<String> datas2 = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
//            @Override
//            public String call(Tuple2<ImmutableBytesWritable, Result> value ) throws Exception {
//                Result result = value._2();
//                byte[] o = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("art"));
//                return null;
//            }
//        });
//        625 m = 1000     696ms=2000    764ms = 5000   共计：48155
//          49ms =500   28ms=1000    27ms =2000  26ms =2500 25ms=3000  24ms =8000  24ms =10000   23ms = 15000 * 2

    }

    static String converScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    /**
     * 日期格式字符串转换成时间戳
     *
     * @param date_str 字符串日期
     * @param format   如：yyyy-MM-dd HH:mm:ss
     * @return   98450
     */
    public static String date2TimeStamp(String date_str, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(date_str).getTime() / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
