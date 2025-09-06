package com.basic.im.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * description: MoneyUtils <br>
 * date: 2020/3/3 0003  <br>
 * author: lidaye <br>
 * version: 1.0 <br>
 */
public class MoneyUtil {

    public static String fromYuan(String money) {
        return new BigDecimal(money).stripTrailingZeros().toPlainString();
    }

    public static String fromCent(String money) {
        return fromCent(Integer.parseInt(money));
    }

    public static String fromCent(int money) {
        BigDecimal ret = new BigDecimal(money);
        ret = ret.divide(new BigDecimal(100), 2, RoundingMode.UNNECESSARY);
        return ret.stripTrailingZeros().toPlainString();
    }

    public static String fromCent(long money) {
        BigDecimal ret = new BigDecimal(money);
        ret = ret.divide(new BigDecimal(100), 2, RoundingMode.UNNECESSARY);
        return ret.stripTrailingZeros().toPlainString();
    }

    public static double fromCentToDouble(long money) {
        return Double.parseDouble(fromCent(money));
    }
    public static Double fromCentToDouble(int money) {
        return Double.parseDouble(fromCent(money));
    }
    public static double fromCentToDouble(String money) {
        return Double.parseDouble(fromCent(money));
    }

    public static  long bigDecimalToLong(BigDecimal from){
      return  from.multiply(new BigDecimal(100)).longValue();
    }

    public static long fromYuanToCent(String money) {
        return new BigDecimal(money).multiply(new BigDecimal(100)).stripTrailingZeros().longValue();
    }
    public static long fromYuanToCent(BigDecimal from) {
        return from.multiply(new BigDecimal(100)).stripTrailingZeros().longValue();
    }
    public static long fromYuanToCentStr(String money) {
        return new BigDecimal(money).multiply(new BigDecimal(100)).stripTrailingZeros().longValue();
    }

    public static Integer fromYuan(Double money){
        if (money > 0.0){
            money = money * 100;
            return money.intValue();
        }
        return 0;
    }

    /**
     * 将Integer类型数组转为double类型数组并且转为 元
     */
    public static List<Double> convertDoubleList(Collection<Integer> interMoneyList) {
        List<Double> moneyList = new ArrayList<>();
        interMoneyList.forEach(obj->moneyList.add(MoneyUtil.fromCentToDouble(obj)));
        return moneyList;
    }


    public static double formatMoneyScale(Double money){
        if (money==null){
            return 0.0;
        }
        if (money<0){
            return money;
        }
        return formatMoneyScale(money,2);
    }

    /**
     * 自定义保留小数位
     */
    public static double formatMoneyScale(Double money,int scale){
        BigDecimal moneyBig = new BigDecimal(money);
        return moneyBig.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    public static double formatMoneyScale(BigDecimal money,int scale){
        return money.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

}
