package quant.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BigDecimalUtil {

    /**
     * BigDecimal 求最大值
     *
     * @param args 待比较参数
     * @return 传入值中最大的 BigDecimal
     */
    public static BigDecimal max(BigDecimal... args) {
        BigDecimal maxVal = args[0];
        for (BigDecimal arg : args) {
            if (arg.compareTo(maxVal) > 0) {
                maxVal = arg;
            }
        }
        return maxVal;
    }

    /**
     * BigDecimal 求最小值
     *
     * @param args 待比较参数
     * @return 传入值中最大的 BigDecimal
     */
    public static BigDecimal min(BigDecimal... args) {
        BigDecimal minVal = args[0];
        for (BigDecimal arg : args) {
            if (arg.compareTo(minVal) < 0) {
                minVal = arg;
            }
        }
        return minVal;
    }

    /**
     * 随机数值
     * @param lower 下界
     * @param upper 上界
     * @param scale 精度
     * @return 随机值
     */
    public static BigDecimal randomBigdecimal(BigDecimal lower, BigDecimal upper, Integer scale){
        BigDecimal result = lower.add(upper.subtract(lower).multiply(new BigDecimal(Math.random())).divide(BigDecimal.ONE, scale, RoundingMode.HALF_DOWN));
        if(result.equals(upper)){
            return lower.add(upper).divide(new BigDecimal("2"));
        }else {
            return result;
        }
    }


}
