package com.StatisticAnalyze_MySQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.sql.*;
import java.util.UUID;

public class GetTicketAndBaseStaion_tb implements Serializable {

    static String selectsql = null;
    static ResultSet retsult = null;

    public static final String url = "jdbc:mysql://192.168.13.204/jdyp_depot?useSSL=false";
    public static final String name = "com.mysql.jdbc.Driver";
    public static final String user = "jdyp";
    public static final String password = "keyway123";

    public static Connection conn = null;
    public static PreparedStatement pst = null;



    //删除表
    public void DropTable(String dropsql) {

        try {
            Class.forName(name);//指定连接类型
            conn = DriverManager.getConnection(url, user, password);//获取连接
            Statement stmt = conn.createStatement();
            stmt.execute(dropsql);
            conn.close();//关闭连接
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //获取基站大表和话单详情表
    public Dataset getBigtableTest() {

        String sql = "(SELECT " +
                "t.ticket_id," +
                "t.other_number," +
                "t.Lai ," +
//                "t.ci ," +
                "b.CI," +
                "b.AC," +
                "b.MNC ," +
                "b.Lat ," +
                "b.Lng ," +
                "b.Address  \n" +
                "FROM bs_460 b,ticket_details t \n" +
                "WHERE t.ci = b.CI and t.Lai = b.AC ) as b1 ";

        Dataset bigdata_table = DataInit.sparkSession.read().format("jdbc")
                .option("url", DataInit.topic_URL)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", sql)
                .option("user", "jdyp")
                .option("password", "keyway123").load();

        bigdata_table.cache();
        return bigdata_table;
    }

    //话单部分字段表
    public Dataset<Row> getTicketDT() {
        //识别运营商
        DataInit.sparkSession.udf().register("ChinaMobilePhoneNum", new UDF1<Long, Integer>() {

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
        Dataset<Row> ticketDT = getBigtableTest();
        ticketDT.createOrReplaceTempView("ticket");
        Dataset<Row> filedsDT = DataInit.sparkSession.sql("select \n" +
                "ticket_id," +
                "ChinaMobilePhoneNum(other_number) code," +
                "Lai," +
                "ci \n" +
                "from ticket");
        filedsDT.cache();

        return filedsDT;
    }

    //话单去重表
    public Dataset<Row> getDistinctTb() {
        Dataset<Row> distinctDT = getTicketDT();
        distinctDT.createOrReplaceTempView("ticket_fileds");
        Dataset<Row> distinct_tb = DataInit.sparkSession.sql("select  " +
                "distinct " +
                "code," +
                "lai," +
                "ci " +
                "from ticket_fileds");
        return distinct_tb;
    }

    //话单统计表
    public Dataset<Row> getGroupTb() {
        Dataset<Row> groupDT = getTicketDT();
        groupDT.createOrReplaceTempView("ticket_fileds");
        Dataset<Row> group_tb = DataInit.sparkSession.sql("select \n" +
                "count(ticket_id) cot," +
                "ticket_id," +
                "lai," +
                "ci," +
                "code  \n" +
                "from ticket_fileds  \n" +
                "group by lai,ci,code,ticket_id ");
        return group_tb;
    }

    public void bsMatchtb() {

        DataInit.sparkSession.udf().register("UUID", new UDF1<Integer, String>() {

            @Override
            public String call(Integer integer) throws Exception {
                return UUID.randomUUID().toString().replace("-", "").toUpperCase();
            }
        }, DataTypes.StringType);

        //获取话单去重表
        Dataset<Row> distinctTB = getDistinctTb();
        distinctTB.createOrReplaceTempView("distinct_tb");

        //获取bs表中字段
        Dataset<Row> bs_DT = getBigtableTest();
        bs_DT.createOrReplaceTempView("bigtable");
        Dataset<Row> bsSmall_tb = DataInit.sparkSession.sql("select Address,Lng,Lat,MNC,AC,CI from bigtable");
        bsSmall_tb.createOrReplaceTempView("bsSmall_tb");

        Dataset<Row> base_station = DataInit.sparkSession.sql("SELECT " +
                "UUID(10) base_station_id," +
                "b.CI plot_code," +
                "b.AC station_code," +
                "b.MNC operator, " +
                "b.Lat latitude," +
                "b.Lng longitude, " +
                "b.Address station_place \n" +
                "FROM bsSmall_tb b, distinct_tb t \n" +
                "WHERE b.AC=t.lai and b.CI=t.ci and b.MNC = t.code");
        base_station.write().mode(SaveMode.Append).jdbc(DataInit.ResultURL, "Bigdata_base_station", DataInit.properties);

        Dataset<Row> groupDT = getGroupTb();
        groupDT.createOrReplaceTempView("group_count_tb");
        Dataset<Row> station_frequency = DataInit.sparkSession.sql("SELECT \n" +
                "UUID(10) station_frequency_id," +
                "g.ticket_id," +
                "b.AC station_code," +
                "b.CI  plot_code," +
                "b.MNC operator," +
                "g.cot call_num," +
                "b.Lat latitude," +
                "b.Lng longitude," +
                "b.Address station_place \n" +
                "FROM bsSmall_tb b,group_count_tb g \n" +
                "WHERE b.AC=g.lai and b.CI=g.ci and b.MNC=g.code");

        station_frequency.write().mode(SaveMode.Append).jdbc(DataInit.ResultURL, "bigdata_station_frequency", DataInit.properties);

    }
}
