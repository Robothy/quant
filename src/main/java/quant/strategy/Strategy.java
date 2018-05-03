package quant.strategy;

import java.util.Map;

/**
 * 交易策略接口，所有的交易策略都得实现此接口。
 * @author robothy
 *
 */
public interface Strategy {
	
	/**
	 * 执行相应策略的入口。
	 * 
	 * @param parameters 策略所需要用到的参数。
	 */
	void run(Map<String, Object> parameters);
	
}
