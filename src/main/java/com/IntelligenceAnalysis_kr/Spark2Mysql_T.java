package com.IntelligenceAnalysis_kr;

import org.apache.derby.impl.sql.execute.StatementTriggerExecutor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import parquet.hadoop.codec.SnappyCodec;

import javax.validation.constraints.Max;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK;

/**
 * 话单详情表（ticket_details）与基站表（bs_460）
 * 匹配：USER_NUMBER() =  MNC
 * lAI = AC
 * ci =CI
 * 输出表：base_station
 */

public class Spark2Mysql_T {

    public static SparkConf Sconf;
    public static SparkSession sparkSession;
    public static JavaSparkContext sc;
    public static SQLContext sqlContext;
    public static Properties properties = new Properties();

    //匹配条件表
    public static String ticket_condition = "ticket_condition";

    public static String URL = "jdbc:mysql://192.168.13.204:3306/jdyp_topic?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

    public static String ResultURL = "jdbc:mysql://192.168.13.204:3306/jdyp_depot?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" +
            "rewriteBatchedStatements=true";

    static {
        Sconf = new SparkConf()
                .set("spark.executor.memory", "4000m")
                .set("spark.driver.memory", "1024m")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.cores.max", "2")
                .setAppName("sparkReadHbaseData")
                .setMaster("local[2]");

        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");

        sparkSession = SparkSession.builder().config(Sconf)
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
                .config("spark.sql.parquet.compression.codec", "snappy").getOrCreate();
        sc = new JavaSparkContext(sparkSession.sparkContext());
        sqlContext = new SQLContext(sc);

        properties.put("user", "jdyp");
        properties.put("password", "keyway123");
        properties.put("driver", "com.mysql.jdbc.Driver");
    }


    /**
     * 获取 bs_460表
     *
     * @param sql
     * @return
     */
    public static Dataset<String> getbs_460(String sql) {

        getTicketTb();

        Dataset bsDT = sqlContext.read().format("jdbc")
                .option("url", URL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", sql)
                .option("user", "jdyp")
                .option("password", "keyway123").load();

        return bsDT;

    }


    /**
     * 获取 ticket 表
     *
     * @param qualifier1
     * @return
     */

    public static void getTicketTb(/*String qualifier1,String qualifier2,String qualifier3*/) {

//        if (StringUtils.isEmpty(qualifier1)){
//            qualifier1.replace("null"," ");
//        }
//        if (StringUtils.isEmpty(qualifier2)){
//            qualifier2.replace("null"," ");
//        }
        //识别运营商 udf
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
//                    System.out.println("号码有误，不足11位。");
                }
                return 404;
            }
        }, DataTypes.IntegerType);

        Dataset ticketDT = sqlContext.read().format("jdbc")
                .option("url", URL)
                .option("driver", "com.mysql.jdbc.Driver")
                // where  event_type_one != '"+qualifier1+"' and  limit 100000
                .option("dbtable", "(select lai,ci,other_number,ticket_id from ticket_details) as T")
                .option("user", "jdyp").option("password", "keyway123").load();

        ticketDT.createOrReplaceTempView("ticket_details");

        //识别运营商
//        String sql = "insert into dev.'" + ticket_condition + "' " +
        String sql = "select " +
                "ChinaMobilePhoneNum(other_number) code," +
                "lai," +
                "ci," +
                "ticket_id " +
                "from ticket_details";
        Dataset Identify_operator_tb = sqlContext.sql(sql);
        Identify_operator_tb.createOrReplaceTempView("Identify_operator_tb");

        //去重表
        Dataset<Row> distinct_tb = sqlContext.sql(" select " +
                "distinct " +
                "code," +
                "lai," +
                "ci " +
                "from Identify_operator_tb");
        distinct_tb.write().mode(SaveMode.Append).jdbc(URL, "distinct_tb", properties);


        //分组统计表
        Dataset<Row> group_count_tb = sqlContext.sql(" select " +
                "count(ticket_id) cot," +
                "ticket_id," +
                "lai," +
                "ci," +
                "code  \n" +
                "from Identify_operator_tb  \n" +
                "group by lai,ci,code,ticket_id ");
        group_count_tb.write().mode(SaveMode.Append).jdbc(URL, "group_count_tb", properties);

//        group_count_tb.createOrReplaceTempView("group_count_tb");

//        numb_code.write().mode(SaveMode.Overwrite).jdbc(URL, ticket_condition, properties);


    }

    public static void main(String[] args) {

//        UUID
        sqlContext.udf().register("UUID", new UDF1<Integer, String>() {

            @Override
            public String call(Integer integer) throws Exception {
                return UUID.randomUUID().toString().replace("-", "").toUpperCase();
            }
        }, DataTypes.StringType);

        String sql_station = "(SELECT " +
//                "UUID(10) base_station_id," +
                "b.CI plot_code," +
                "b.AC station_code," +
                "b.MNC operator, " +
                "b.Lat latitude," +
                "b.Lng longitude, " +
                "b.Address station_place \n" +
                "FROM bs_460 b, distinct_tb t \n" +
                "WHERE b.AC=t.lai and b.CI=t.ci and b.MNC = t.code ) as t";
        sqlContext.sql("set mapred.output.compress=true");

        Dataset<String> base_station = getbs_460(sql_station);
        base_station.write().mode(SaveMode.Append).jdbc(ResultURL, "bigdata_base_station", properties);

        String sql_frequency = "(SELECT \n" +
                "g.ticket_id," +
                "b.AC station_code," +
                "b.CI  plot_code," +
                "b.MNC operator," +
                "g.cot call_num," +
                "b.Lat latitude," +
                "b.Lng longitude," +
                "b.Address station_place \n" +
                "FROM bs_460 b,group_count_tb g \n" +
                "WHERE b.AC=g.lai and b.CI=g.ci and b.MNC=g.code) as t1";

        Dataset bigdata_station_frequency = getbs_460(sql_frequency);
        bigdata_station_frequency.write().mode(SaveMode.Append).jdbc(ResultURL, "bigdata_station_frequency", properties);


        sparkSession.close();
        sc.stop();
    }
}
