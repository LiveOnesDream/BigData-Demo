package com.IntelligenceAnalysis_kr;

public class test {
    String YD = "^[1]{1}(([3]{1}[4-9]{1})|([5]{1}[012789]{1})|([8]{1}[23478]{1})|([4]{1}[7]{1})|([7]{1}[8]{1}))[0-9]{8}$";
    String LT = "^[1]{1}(([3]{1}[0-2]{1})|([5]{1}[56]{1})|([8]{1}[56]{1})|([4]{1}[5]{1})|([7]{1}[6]{1}))[0-9]{8}$";
    String DX = "^[1]{1}(([3]{1}[3]{1})|([5]{1}[3]{1})|([8]{1}[09]{1})|([7]{1}[37]{1}))[0-9]{8}$";
    public Integer call(Long mobPhnNum) throws Exception {
        // 判断手机号码是否是11位
        if (mobPhnNum.toString().length() == 11) {
            // 判断手机号码是否符合中国移动的号码规则
            if (mobPhnNum.toString().matches(YD)) {
                return 1;
            }
            // 判断手机号码是否符合中国联通的号码规则
            else if (mobPhnNum.toString().matches(LT)) {
                return 2;
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
        return 0;
    }

    public static void main(String[] args) {

    }
}
