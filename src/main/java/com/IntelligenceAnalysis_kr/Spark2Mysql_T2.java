package com.IntelligenceAnalysis_kr;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class Spark2Mysql_T2 {

    public static SparkConf Sconf;
    public static SQLContext sqlContext;
    public static SparkSession sparkSession;
    public static JavaSparkContext sc;
    public static Properties properties;
    //基站表
    public static String tb_bs = "bs";
    //统计结果表
    public static String s_base_station = "s_base_station";

    public static String ticketURL = "jdbc:mysql://192.168.13.204:3306/jdyp_topic?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

    public static String bsURL = "jdbc:mysql://192.168.13.204:3306/jdyp_topic?useSSL=false&useUnicode=true" +
            "&characterEncoding=utf8&" + "rewriteBatchedStatements=true";

    static {

        Sconf = new SparkConf()
                .set("spark.executor.memory", "2000m")
                .setAppName("sparkReadHbaseData")
                .set("spark.serlializer", "org.apache.spark.serializer.JavaSerializer")
                .setMaster("local[2]")
                .set("spark.sql.inMemoryColumnarStorage.compressed", String.valueOf(true));
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        Sconf.set("spark.cores.max", "3");

        sparkSession = SparkSession.builder().config(Sconf).getOrCreate();
        sc = new JavaSparkContext(sparkSession.sparkContext());
        sqlContext = new SQLContext(sc);

        properties = new Properties();
        properties.put("user", "jdyp");
        properties.put("password", "keyway123");
        properties.put("driver", "com.mysql.jdbc.Driver");

    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        Dataset<String> ticketDT = getTicketTb("主叫");
        ticketDT.createOrReplaceTempView("ticket_view");

        //  创建基站详情临时表  bs_view
        Dataset<String> bsDT = getbsTb();
        bsDT.createOrReplaceTempView("bs_view");

        //话单表和基站表关联
        Dataset resDT = sqlContext.sql("select " +
                " a.CI plot_code," +
                "a.lai station_code," +
                " b.Lat latitude," +
                "b.Lng longitude," +
                "a.code operator," +
                "b.Address station_place \n" +
                "from ticket_view  a \n" +
                "left join bs_view b on a.lai=b.AC and a.ci=b.CI and a.code=b.MNC");

        resDT.show(10000);
//        resDT.write().mode(SaveMode.Append).jdbc(bsURL, s_base_station, properties);
        System.out.println("插数据用时:" + (System.currentTimeMillis() - start) / 1000 + "s");
        sc.stop();
//        sqlContext.sql("select  ChinaMobilePhoneNum(user_number) from ticket_details").show();
//        Dataset resultDT = sqlContext.sql("select\n" +
//                "       b.mnc,\n" +
//                "      b.address, a.lai,\n" +
//                "      a.ci,\n" +
//                "      b.lat,\n" +
//                "      b.lng from (select distinct lai,ci from ticket_details) a left join bs b on a.lai=b.ac and a.ci=b.ci ");
//        resultDT.show();
//        String resultTable = "s_base_station";
//        resultDT.write().mode(SaveMode.Append).jdbc(ticketURL, resultTable, properties);

        //基站表  话单详情表
//        JavaRDD<String> ticketRDD = ticketDT.javaRDD().map(new Function() {
//            @Override
//            public Object call(Object v1) throws Exception {
//                String value = v1.toString();
//                return ChinaMobilePhoneNum.matchNum(value);
//            }
//        });


//        ticketRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
//            @Override
//            public void call(Iterator<String> v) throws Exception {
//                while (v.hasNext()) {
//                    String code = v.next();
//                    System.out.println(code);
//                }
//            }
//        });

    }

    /**
     * 读取大表  获取 lai,ci,user_number 创建临时表
     * 识别user_number运营商 并去重
     *
     * @param qualifier1
     * @return numb_code
     */

    public static Dataset<String> getTicketTb(String qualifier1/*,String qualifier2,String qualifier3*/) {

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
                    System.out.println("号码有误，不足11位。");
                }
                return 404;
            }
        }, DataTypes.IntegerType);
        Dataset ticketDT = sqlContext.read().format("jdbc")
                .option("url", ticketURL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "(select lai,ci,other_number from ticket_details where  event_type_one != '\"+qualifier1+\"' and  limit 100000 ) as T")
                .option("user", "jdyp").option("password", "keyway123").load();
        ticketDT.createOrReplaceTempView("threeFiled");
        Dataset numb_code = sqlContext.sql("select distinct ChinaMobilePhoneNum(other_number) code,lai ,ci  from threeFiled");

        return numb_code;
    }

    /**
     * 获取基站表中 AC CI Lat Address MNC 字段
     *
     * @return bsDT
     */
    public static Dataset<String> getbsTb() {
        Dataset bsDT = sqlContext.read().format("jdbc")
                .option("url", bsURL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "(select AC,CI,Lat ,Lng,Address,MNC from bs_460 where AC =) as b")
                .option("user", "jdyp").option("password", "keyway123").load();
        return bsDT;
    }
}
