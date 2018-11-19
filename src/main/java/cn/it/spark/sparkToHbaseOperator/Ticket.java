package cn.it.spark.sparkToHbaseOperator;

import java.io.Serializable;

public class Ticket implements Serializable {
    //    CI IMEI IMSI LAI 事件类型 事件类型b 呼叫日期 呼叫时长 呼叫时间 基站地址 基站地址b
//     对方归属地 用户号码 用户归属地 电话号码 记录类型 记录类型b 通话所在地区号

    private String CI;
    private String IMEI;
    private String IMSI;
    private String LAI;
    private String 事件类型;
    private String 事件类型b;
    private String 呼叫日期;
    private String 呼叫时长;
    private String 呼叫时间;
    private String 基站地址;
    private String 基站地址b;
    private String 对方归属地;
    private String 用户号码;
    private String 用户归属地;
    private String 电话号码;
    private String recode_type;
    private String 记录类型b;
    private String 通话所在地区号;

    public Ticket() {
    }

    public Ticket(String CI, String IMEI, String IMSI, String LAI, String 事件类型, String 事件类型b, String 呼叫日期,
                  String 呼叫时长, String 呼叫时间, String 基站地址, String 基站地址b, String 对方归属地, String 用户号码,
                  String 用户归属地, String 电话号码, String recode_type, String 记录类型b, String 通话所在地区号) {
        this.CI = CI;
        this.IMEI = IMEI;
        this.IMSI = IMSI;
        this.LAI = LAI;
        this.事件类型 = 事件类型;
        this.事件类型b = 事件类型b;
        this.呼叫日期 = 呼叫日期;
        this.呼叫时长 = 呼叫时长;
        this.呼叫时间 = 呼叫时间;
        this.基站地址 = 基站地址;
        this.基站地址b = 基站地址b;
        this.对方归属地 = 对方归属地;
        this.用户号码 = 用户号码;
        this.用户归属地 = 用户归属地;
        this.电话号码 = 电话号码;
        this.recode_type = recode_type;
        this.记录类型b = 记录类型b;
        this.通话所在地区号 = 通话所在地区号;
    }

    public String getCI() {
        return CI;
    }

    public void setCI(String CI) {
        this.CI = CI;
    }

    public String getIMEI() {
        return IMEI;
    }

    public void setIMEI(String IMEI) {
        this.IMEI = IMEI;
    }

    public String getIMSI() {
        return IMSI;
    }

    public void setIMSI(String IMSI) {
        this.IMSI = IMSI;
    }

    public String getLAI() {
        return LAI;
    }

    public void setLAI(String LAI) {
        this.LAI = LAI;
    }

    public String get事件类型() {
        return 事件类型;
    }

    public void set事件类型(String 事件类型) {
        this.事件类型 = 事件类型;
    }

    public String get事件类型b() {
        return 事件类型b;
    }

    public void set事件类型b(String 事件类型b) {
        this.事件类型b = 事件类型b;
    }

    public String get呼叫日期() {
        return 呼叫日期;
    }

    public void set呼叫日期(String 呼叫日期) {
        this.呼叫日期 = 呼叫日期;
    }

    public String get呼叫时长() {
        return 呼叫时长;
    }

    public void set呼叫时长(String 呼叫时长) {
        this.呼叫时长 = 呼叫时长;
    }

    public String get呼叫时间() {
        return 呼叫时间;
    }

    public void set呼叫时间(String 呼叫时间) {
        this.呼叫时间 = 呼叫时间;
    }

    public String get基站地址() {
        return 基站地址;
    }

    public void set基站地址(String 基站地址) {
        this.基站地址 = 基站地址;
    }

    public String get基站地址b() {
        return 基站地址b;
    }

    public void set基站地址b(String 基站地址b) {
        this.基站地址b = 基站地址b;
    }

    public String get对方归属地() {
        return 对方归属地;
    }

    public void set对方归属地(String 对方归属地) {
        this.对方归属地 = 对方归属地;
    }

    public String get用户号码() {
        return 用户号码;
    }

    public void set用户号码(String 用户号码) {
        this.用户号码 = 用户号码;
    }

    public String get用户归属地() {
        return 用户归属地;
    }

    public void set用户归属地(String 用户归属地) {
        this.用户归属地 = 用户归属地;
    }

    public String get电话号码() {
        return 电话号码;
    }

    public void set电话号码(String 电话号码) {
        this.电话号码 = 电话号码;
    }

    public String getrecode_type() {
        return recode_type;
    }

    public void setrecode_type(String recode_type) {
        this.recode_type = recode_type;
    }

    public String get记录类型b() {
        return 记录类型b;
    }

    public void set记录类型b(String 记录类型b) {
        this.记录类型b = 记录类型b;
    }

    public String get通话所在地区号() {
        return 通话所在地区号;
    }

    public void set通话所在地区号(String 通话所在地区号) {
        this.通话所在地区号 = 通话所在地区号;
    }

    @Override
    public String toString() {
        return "Ticket{" +
                "CI='" + CI + '\'' +
                ", IMEI='" + IMEI + '\'' +
                ", IMSI='" + IMSI + '\'' +
                ", LAI='" + LAI + '\'' +
                ", 事件类型='" + 事件类型 + '\'' +
                ", 事件类型b='" + 事件类型b + '\'' +
                ", 呼叫日期='" + 呼叫日期 + '\'' +
                ", 呼叫时长='" + 呼叫时长 + '\'' +
                ", 呼叫时间='" + 呼叫时间 + '\'' +
                ", 基站地址='" + 基站地址 + '\'' +
                ", 基站地址b='" + 基站地址b + '\'' +
                ", 对方归属地='" + 对方归属地 + '\'' +
                ", 用户号码='" + 用户号码 + '\'' +
                ", 用户归属地='" + 用户归属地 + '\'' +
                ", 电话号码='" + 电话号码 + '\'' +
                ", recode_type='" + recode_type + '\'' +
                ", 记录类型b='" + 记录类型b + '\'' +
                ", 通话所在地区号='" + 通话所在地区号 + '\'' +
                '}';
    }
}
