package com.IntelligenceAnalysis_kr;


import jxl.biff.DataValiditySettingsRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.derby.vti.IFastPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
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

public class Spark2HBase_T {

    public static SparkConf Sconf;
    public static SparkSession sparkSession;
    public static JavaSparkContext sc;
    public static Configuration hconf;

    //mysql url
    public static String url = "jdbc:mysql://localhost:3306/jdyp_depot?useSSL=false&useUnicode=true&characterEncoding=utf8&" +
            "rewriteBatchedStatements=true";
    //要获取的HBASE表名
    public static String tableName = "Ticket_Hfile_TEST";
    // spark  HBASE 配置连接信息
    static {
        Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.cores.max", "2")
                .setAppName("sparkReadHbaseData")
                .setMaster("local[2]");
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        sc = new JavaSparkContext(sparkSession.sparkContext());

        //HBASE连接  配置zookeeper  ....
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        hconf.set("hbase.zookeeper.quorum", "192.168.65.154");
        hconf.set("hbase.master", "192.168.65.154:16030");
        hconf.set("hbase.rootdir", "/hbase");

        try {
            Scan scan = new Scan();
            hconf.set(TableInputFormat.SCAN, converScanToString(scan));
            hconf.set(TableInputFormat.INPUT_TABLE, tableName);
            hconf.set(TableInputFormat.SCAN_CACHEBLOCKS, "cf");
            hconf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {

        String sql = "select * from Ticket_Data2 where other_number != 95533 ";
        insertintoMySQL(sql);
    }

    /**
     * scan 类型转换
     * @param scan
     * @return
     * @throws IOException
     */
    static String converScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    private static void readMySQL(SQLContext sqlContext) {
        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://localhost:3306/test";
        //查找的表名
        String table = "bs_460";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的bs_460表内容");
        // 读取表中所有数据
        Dataset jdbcDF = sqlContext.read().jdbc(url, table, connectionProperties).select("select * from ");
        //显示数据
        jdbcDF.show();
    }


    /**
     * 获取HBASE 话单详情表  并创建schema
     *
     * @return Dataset<String>
     */
    public static Dataset<String> getHbaseTb() {

        //        Job job = Job.getInstance(hconf);
//        Path path = new Path("/hbase-snapshot");
//        String snapName = "Ticket_Hfile_TEST_snap";
//        TableSnapshotInputFormat.setInput(job, snapName, path);
        //获取hbase 话单表，创建临时表Ticket_Data2
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
        Dataset hbaseTable = sparkSession.createDataFrame(recordValueRDD, schema);

        return hbaseTable;
    }

    /**
     * 插数据到mysql
     * @param sql
     */
    public static void insertintoMySQL(String sql) {

        Dataset<String> hbaseTable = getHbaseTb();
        hbaseTable.createOrReplaceTempView("Ticket_Data2");

        //将原表过滤
        Dataset filtertb = sparkSession.sql(sql);

        filtertb.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> row) throws Exception {
                Connection conn = null;
                PreparedStatement stmt = null;
                PreparedStatement stmt1 = null;
                Row row1 = null;

                Properties properties = new Properties();
                properties.setProperty("user", "jdyp");
                properties.setProperty("password", "keyway123");
//                properties.setProperty("user", "root");
//                properties.setProperty("password", "");

                String details_sql = "insert into s_ticket_details (ticket_details_id,ticket_id,record_type_one,record_type_two," +
                        "call_date,call_time,event_type_one,event_type_two,user_number,user_affiliation,imei,imsi,other_number," +
                        "other_affiliation,lai,ci,station_address_one,station_address_two,call_address,create_user,create_time) " +
                        "value (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                String translate_sql = "insert into s_ticket_translate (ticket_translate_id,ticket_id,call_date," +
                        "call_time,imsi,imei,other_number,other_affiliation,lai,ci,owner_station_address,opposite_station_address," +
                        ",call_address,create_user,create_time) value (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";


                try {
                    conn = DriverManager.getConnection(url, properties);
                    conn.setAutoCommit(false);
                    stmt = conn.prepareStatement(details_sql);
                    stmt1 = conn.prepareStatement(translate_sql);
                    int count = 0;

                    while (row.hasNext()) {
                        count++;
                        row1 = row.next();
                        long date = System.currentTimeMillis();
                        String t = String.valueOf(date / 1000);

                        String timeStime = String.valueOf(row1.getAs("call_date")) + " " + String.valueOf(row1.getAs("call_time"));
                        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                        String UUID = "UUID.randomUUID().toString().replace(\"-\", \"\").toUpperCase()";
                        stmt.setString(1, UUID);
                        stmt.setString(2, "");
                        stmt.setString(3, row1.getAs("record_type_one").toString());
                        stmt.setString(4, row1.getAs("record_type_two").toString());
                        stmt.setTimestamp(5, new Timestamp(Long.parseLong(timeStime)));

                        if (StringUtils.isNotEmpty(row1.getAs("call_time"))) {
                            stmt.setLong(6, row1.getAs("call_time"));
                        } else {
                            stmt.setLong(6, 0L);
                        }
                        stmt.setString(7, row1.getAs("event_type_one"));
                        stmt.setString(8, row1.getAs("event_type_two"));

                        String user_number = row1.getAs("user_number").toString();
                        if (StringUtils.isNotEmpty(user_number) && user_number.length() <= 16) {
                            stmt.setLong(9, Long.parseLong(user_number));
                        } else {
                            stmt.setLong(9, 0L);
                        }
                        if (StringUtils.isNotEmpty(row1.getAs("user_affiliation"))) {
                            stmt.setLong(10, Long.parseLong(row1.getAs("user_affiliation").toString()));
                        } else {
                            stmt.setLong(10, 0L);
                        }
                        if (StringUtils.isNotEmpty("imei")) {
                            stmt.setLong(11, Long.parseLong(row1.getAs("imei").toString()));
                        } else {
                            stmt.setLong(11, 0L);
                        }
                        if (StringUtils.isNotEmpty("imsi")) {
                            stmt.setLong(12, Long.parseLong(row1.getAs("imsi").toString()));
                        } else {
                            stmt.setLong(12, 0L);
                        }
                        String other_number = row1.getAs("other_number").toString();
                        if (StringUtils.isNotEmpty(other_number) && other_number.length() <= 16) {
                            stmt.setLong(13, Long.parseLong(other_number));
                        } else {
                            stmt.setLong(13, 0L);
                        }
                        if (StringUtils.isNotEmpty("other_affiliation")) {
                            stmt.setLong(14, Long.parseLong(row1.getAs("other_affiliation").toString()));
                        } else {
                            stmt.setLong(14, 0L);
                        }

                        stmt.setString(15, row1.getAs("lai").toString());
                        stmt.setString(16, row1.getAs("ci").toString());
                        stmt.setString(17, row1.getAs("station_address_one").toString());
                        stmt.setString(18, row1.getAs("station_address_two").toString());
                        stmt.setString(19, row1.getAs("call_address").toString());
                        stmt.setString(20, "");
                        stmt.setString(21, "");

                        stmt.addBatch();
                        stmt.clearParameters();


                        stmt1.setString(1, UUID);
                        stmt1.setString(2, "");
                        String date_str = String.valueOf(row1.getAs("call_date").toString().trim() +
                                " " + row1.getAs("call_time").toString().trim());
//                        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                        stmt1.setTimestamp(3, Timestamp.valueOf(date_str));

                        if (StringUtils.isNotEmpty(row1.getAs("call_time"))) {
                            stmt1.setLong(4, row1.getAs("call_time"));
                        } else {
                            stmt1.setLong(4, 0L);
                        }

                        if (StringUtils.isNotEmpty("imsi")) {
                            stmt1.setLong(5, Long.parseLong(row1.getAs("imsi").toString()));
                        } else {
                            stmt.setLong(5, 0L);
                        }

                        if (StringUtils.isNotEmpty("imei")) {
                            stmt1.setLong(6, Long.parseLong(row1.getAs("call_time").toString()));
                        } else {
                            stmt.setLong(6, 0L);
                        }
                        if (StringUtils.isNotEmpty("other_number")) {
                            stmt1.setLong(7, Long.parseLong(row1.getAs("other_number").toString()));
                        } else {
                            stmt.setLong(7, 0L);
                        }
                        if (StringUtils.isNotEmpty("other_affiliation")) {
                            stmt1.setLong(8, Long.parseLong(row1.getAs("other_affiliation").toString()));
                        } else {
                            stmt.setLong(8, 0L);
                        }
                        stmt1.setString(9, row1.getAs("lai").toString());
                        stmt1.setString(10, row1.getAs("ci").toString());
                        stmt1.setString(11, "");
                        stmt1.setString(12, "");
                        stmt1.setString(13, "");
                        stmt1.setString(14, "");
                        stmt1.setString(15, "");

                        stmt1.addBatch();
                        stmt1.clearParameters();

                        if (count % 2000 == 0) {
                            stmt.executeBatch();
                            stmt1.executeBatch();
                            conn.commit();
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
                    if (stmt1 != null) {
                        stmt1.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
        });
        sc.close();
    }

}
