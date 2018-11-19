package com.IntelligenceAnalysis_kr;

import com.twitter.chill.java.UUIDSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.datanucleus.state.LifeCycleState;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class Spark2Mysql_T3 {

    public static SparkConf Sconf;
    public static Properties properties;

    public static String ticketURL = "jdbc:mysql://192.168.13.204:3306/jdyp_topic?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

    public static String bsURL = "jdbc:mysql://192.168.13.204:3306/jdyp_topic?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" +
            "rewriteBatchedStatements=true";

    static {

        Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]")
                .set("spark.sql.inMemoryColumnarStorage.compressed", String.valueOf(true));
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "3");

        properties = new Properties();
        properties.put("user", "jdyp");
        properties.put("password", "keyway123");
        properties.put("driver", "com.mysql.jdbc.Driver");

    }

    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sc);

//        String num = "a";
//        Dataset<String> testTd = getFiltertb(num);

        //uuid
        sqlContext.udf().register("UUID", new UDF1<Integer, String>() {

            @Override
            public String call(Integer integer) throws Exception {
                return UUID.randomUUID().toString().replace("-", "").toUpperCase();
            }
        }, DataTypes.StringType);

        //识别运营商
        sqlContext.udf().register("ChinaMobilePhoneNum", new UDF1<Long, Integer>() {

            String YD = "^[1]{1}(([3]{1}[4-9]{1})|([5]{1}[012789]{1})|([8]{1}[23478]{1})|([4]{1}[7]{1})|([7]{1}[8]{1}))[0-9]{8}$";
            String LT = "^[1]{1}(([3]{1}[0-2]{1})|([5]{1}[56]{1})|([8]{1}[56]{1})|([4]{1}[5]{1})|([7]{1}[6]{1}))[0-9]{8}$";
            String DX = "^[1]{1}(([3]{1}[3]{1})|([5]{1}[3]{1})|([8]{1}[09]{1})|([7]{1}[37]{1}))[0-9]{8}$";

            @Override
            public Integer call(Long mobPhnNum) throws Exception {
                // 判断手机号码是否是11位
                if (mobPhnNum.toString().length() == 11) {
                    // 判断手机号码是否符合中国移动的号码规则
                    if (mobPhnNum.toString().matches(YD)) {
                        return 0;
                    }
                    // 判断手机号码是否符合中国联通的号码规则
                    else if (mobPhnNum.toString().matches(LT)) {
                        return 1;
                    }
                    // 判断手机号码是否符合中国电信的号码规则
                    else if (mobPhnNum.toString().matches(DX)) {
                        return 11;
                    }
                }
                // 不是11位
                else {
                    System.out.println("号码有误，不足11位。");
                }
                return 404;
            }
        }, DataTypes.IntegerType);

        //获取话单表 只取字段: ticket_id,lai,ci,user_num
        String sql = "(select ticket_id,lai,ci,user_number from ticket_details  ) as T";
        Dataset ticketDT = sqlContext.read().format("jdbc").option("url", ticketURL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", sql)
                .option("user", "jdyp").option("password", "keyway123")
                .load();

        //创建 包含 lai,ci,user_number ,4个字段临时表
        ticketDT.createOrReplaceTempView("fourField");

        ticketDT.sqlContext().cacheTable("fourField");

        //识别user_number运营商  0 移动 1 联通 11 电信
        Dataset tmptb = sqlContext.sql("select ChinaMobilePhoneNum(user_number) code,lai,ci,ticket_id from fourField");

        tmptb.createOrReplaceTempView("tmptb");

        //  去重并创建临时表
        Dataset numb_code = sqlContext.sql("select " +
                "distinct " +
                "code," +
                "lai," +
                "ci " +
                "from tmptb");
        numb_code.createOrReplaceTempView("ticket_view");

        //将话单详情进行count 和group by
        Dataset grouptb = sqlContext.sql("select " +
                "count(ticket_id) cot," +
                "ticket_id," +
                "lai," +
                "ci," +
                "code  \n" +
                "from tmptb  \n" +
                "group by lai,ci,code,ticket_id ");

        grouptb.createOrReplaceTempView("grouptb");


        //连接jdyp_topic库  创建基站详情临时表  bs_view
        String tb_bs = "bs";
        String bsSQL = "(select AC,CI,Lat,Lng,Address,MNC from bs_460) as b";
        Dataset bsDT = sqlContext.read().format("jdbc").option("url", bsURL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", bsSQL)
                .option("user", "jdyp").option("password", "keyway123").load();
        bsDT.createOrReplaceTempView("bs_view");


        //话单表和基站表关联   生成s_base_station表
        Dataset base_stationDT = sqlContext.sql("select \n" +
                "UUID(11) base_station_id," +
                "a.CI plot_code," +
                "a.lai station_code," +
                "a.code operator," +
                "b.Lat latitude," +
                "b.Lng longitude," +
                "b.Address station_place \n" +
                "from ticket_view  a \n" +
                "left join bs_view b on a.lai=b.AC and a.ci=b.CI and a.code=b.MNC");

        // 话单表和基站表关联 生成s_station_frequency表
        Dataset station_frequencyDT = sqlContext.sql("SELECT \n" +
                "UUID(10) station_frequency_id," +
                "g.ticket_id ," +
                "g.lai station_code," +
                "g.ci  plot_code ," +
                "g.code operator ," +
                "g.cot call_num ," +
                "b.Lat latitude ," +
                "b.Lng longitude ," +
                "b.Address station_place \n" +
                "from grouptb g \n" +
                "left join bs_view b on g.lai=b.AC and g.ci=b.CI and g.code=b.MNC");
        station_frequencyDT.javaRDD().saveAsTextFile("hdfs://ip:");


        System.out.println("将统计结果插入mysql");

        String ResultURL = "jdbc:mysql://192.168.13.204:3306/jdyp_depot?useSSL=false&useUnicode=true" +
                "&characterEncoding=utf8&" +
                "rewriteBatchedStatements=true";

        //写数据到s_base_station表
        String bigdata_base_station = "bigdata_base_station";
        base_stationDT.write().mode(SaveMode.Append).jdbc(ResultURL, bigdata_base_station, properties);


        //写数据到s_station_frequency表
        String bigdata_station_frequency = "s_station_frequency";
        station_frequencyDT.write().mode(SaveMode.Append).jdbc(ResultURL, bigdata_station_frequency, properties);


        long end = System.currentTimeMillis();
        System.out.println("插数据用时:" + (end - start) / 1000 + "ms");

        sc.stop();

    }

    public static String getUUID(int num) {
        String uuid = null;
        for (int i = 0; i < num; i++) {
            uuid = UUID.randomUUID().toString().replace("-", "").toUpperCase();
        }
        return uuid;
    }

    /**
     * 传参获取清洗标准
     * @param qualifier1
     * @return
     */
    public static Dataset<String> getFiltertb(String qualifier1) {

        SparkSession sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sc);
        String sql = "(select ticket_id,lai,ci,user_number from ticket_details where op != '\"+qualifier1+\"') as T";
        Dataset ticketDT = sqlContext.read().format("jdbc").option("url", ticketURL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", sql)
                .option("user", "jdyp").option("password", "keyway123")
                .load();

        return ticketDT;
    }
}
