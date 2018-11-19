package com.IntelligenceAnalysis_kr;


import cn.it.spark.sparkToHbaseOperator.HbaseTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class Spark2HBase {
    public static Scan scan = new Scan();

    public static void main(String[] args) throws IOException {

        SparkConf Sconf = new SparkConf()
                .set("spark.executor.memory", "1000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]");

        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "2");

        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        //HBASE连接  配置zookeeper  ....
        Configuration hconf = HBaseConfiguration.create();

        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        hconf.set("hbase.zookeeper.quorum", "10.3.10.134");
        hconf.set("hbase.master", "10.3.10.134:16030");

        String tableName = "Ticket_Data1";
        //设置查询的表名，开始rowkey，结束rowkey，列族

        hconf.set(TableInputFormat.INPUT_TABLE, tableName);
        hconf.set(TableInputFormat.SCAN, converScanToString(scan));
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

        emp.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> row) throws Exception {
                Connection conn = null;
                PreparedStatement stmt = null;
                PreparedStatement stmt1 = null;
                Row row1 = null;

                String sql = "insert into s_ticket_details_copy (ticket_details_id,ticket_id,record_type_one,record_type_two," +
                        "user_number,other_number) value (?,?,?,?,?,?)";

                String s_sql1 = "insert into s_ticket_translate (ticket_translate_id,ticket_id,call_date," +
                        "call_time,imsi,imei,other_number,other_affiliation,lai,ci,owner_station_address,opposite_station_address," +
                        ",call_address,create_user,create_time) value (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                String sql1 = "insert into s_ticket_translate (ticket_translate_id,ticket_id,call_date," +
                        "call_time) value (?,?,?,?)";

                Properties properties = new Properties();
                properties.setProperty("user", "jdyp");
                properties.setProperty("password", "keyway123");
                String url = "jdbc:mysql://192.168.13.204:3306/jdyp_depot?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true";
                try {
                    String temp = "10659189101495098196";
                    //                    System.out.println("temp: " + new BigDecimal(temp));
                    conn = DriverManager.getConnection(url, properties);
                    conn.setAutoCommit(false);
                    stmt = conn.prepareStatement(sql);
                    stmt1 = conn.prepareStatement(sql1);
                    int count = 0;
                    while (row.hasNext()) {
                        count++;
                        row1 = row.next();
                        long date = System.currentTimeMillis();
                        String t = String.valueOf(date / 1000);

                        String timeStime = row1.getAs("call_date").toString() + row1.getAs("call_time").toString();
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
                        java.util.Date sqldate = format.parse(timeStime);

                        stmt.setString(1, UUID.randomUUID().toString().replace("-", "").toUpperCase());
                        stmt.setString(2, "");
                        stmt.setString(3, row1.getAs("record_type_one").toString());
                        stmt.setString(4, row1.getAs("record_type_two").toString());
                        stmt.setLong(4, sqldate.getTime());
                        String user_number = row1.getAs("user_number").toString();
                        if (StringUtils.isNotEmpty(user_number)) {
                            stmt.setLong(5, Long.parseLong(user_number));
                        } else {
                            stmt.setLong(5, 0L);
                        }
                        String other_number = row1.getAs("other_number").toString();
                        if (StringUtils.isNotEmpty(other_number) && other_number.length() <= 16) {
                            stmt.setLong(6, Long.parseLong(other_number));
                        } else {
                            stmt.setLong(6, 0L);
                        }

                        stmt1.setString(1, UUID.randomUUID().toString().replace("-", "").toUpperCase());
                        stmt1.setString(2, "");
                        String date_str = String.valueOf(row1.getAs("call_date").toString().trim() +
                                " " + row1.getAs("call_time").toString().trim());
                        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                        stmt1.setTimestamp(3, Timestamp.valueOf(date_str));

                        if (StringUtils.isNotEmpty(row1.getAs("call_duration"))) {
                            stmt1.setLong(4, Long.parseLong(row1.getAs("call_duration")));
                        } else {
                            stmt1.setLong(4, 0L);
                        }

                        stmt.addBatch();
                        stmt1.addBatch();

                        stmt.clearParameters();
                        stmt1.clearParameters();

                        if (count % 2000 == 0) {
                            stmt.executeBatch();
                            stmt1.executeBatch();
                        }
                    }
                    stmt.executeBatch();
                    stmt1.executeBatch();

                    conn.commit();

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {

                    if (stmt != null) {
                        stmt.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
        });

        sc.close();

//        FilterList filterList = FilterService();
//        scan.setFilter(filterList);
//        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(HbaseTest.conf,
//                TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//        JavaRDD<String> datas2 = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
//            @Override
//            public String call(Tuple2<ImmutableBytesWritable, Result> value) throws Exception {
//                Result result = value._2();
//                byte[] o = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("art"));
//                return null;
//            }
//        });

    }

    static String converScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    /**
     * hbase 过滤器
     *
     * @return
     */

    public static FilterList FilterService() {

        scan.addColumn("cf".getBytes(), "呼叫日期".getBytes());
        scan.addColumn("cf".getBytes(), "用户归属地".getBytes());
        List<Filter> filters = new ArrayList<>();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"),
                Bytes.toBytes("呼叫时间"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("2017-04-27"));
        filter.setFilterIfMissing(true);//if set true will skip row which column doesn't exist
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("cf"),
                Bytes.toBytes("用户归属地"), CompareFilter.CompareOp.LESS, Bytes.toBytes("0564"));
        filter2.setFilterIfMissing(true);

        PageFilter filter3 = new PageFilter(10);
        filters.add(filter);
        filters.add(filter2);
        filters.add(filter3);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        return filterList;

    }
}
