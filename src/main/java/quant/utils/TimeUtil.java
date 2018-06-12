package quant.utils;

public class TimeUtil {
	
	/**
	 * 延时函数
	 * @param ms 延时时间（单位：毫秒）
	 */
	public static void delay(Long ms){
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
