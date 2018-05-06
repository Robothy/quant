package quant.strategy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;

import org.apache.logging.log4j.Logger;

import exunion.exchange.Exchange;
import exunion.exchange.ExchangeFactory;
import exunion.metaobjects.Depth;
import quant.exchange.EndExchangeFactory;

public class AssemblyHedge implements Strategy {
	
	/*****************************************************************
	 * 
	 * 	静态内部类定义
	 * 
	 ****************************************************************/
	
	/**
	 * 对冲币种对，由于对冲可能是多个平台之间的币种对冲，所以对冲币种对相对于普通的币种对增加平台 plantform 属性
	 * @author robothy
	 *
	 */
	public static class HedgeCurrencyPair{
		private String plantfrom = null;
		private String currencyPair = null;
		public String getCurrencyPair() {
			return currencyPair;
		}
		public void setCurrencyPair(String currencyPair) {
			this.currencyPair = currencyPair;
		}
		public String getPlatform() {
			return plantfrom;
		}
		public void setPlatform(String platform) {
			this.plantfrom = platform;
		}
	}
	
	/*****************************************************************
	 * 
	 * 	全局参数定义
	 * 
	 ****************************************************************/
	
	private static final Logger logger = LogManager.getLogger(AssemblyHedge.class);
	
	// 组合对冲币种对
	private List<HedgeCurrencyPair> hedgeCurrencyPairs = null;
	
	// 交易所实例
	private Map<String, Exchange> exchanges = null;
	
	//API操作失败休息时间， （单位：毫秒），默认 1000ms
	private Long failedSleepTime = 1000L;
	
	//操作周期, 默认10毫秒
	private Long cycle = 10L;
	
	/**
	 * @return 当前策略实例使用到的交易所实例
	 */
	public Map<String, Exchange> getExchanges() {
		return exchanges;
	}

	/**
	 * @return 操作失败时休息时间(单位：毫秒)
	 */
	public Long getFailedSleepTime() {
		return failedSleepTime;
	}

	/**
	 * 设置操作失败休息时间（单位：毫秒）
	 * @param failedSleepTime 操作失败休息时间（单位：毫秒）
	 */
	public AssemblyHedge setFailedSleepTime(Long failedSleepTime) {
		this.failedSleepTime = failedSleepTime;
		return this;
	}

	/**
	 * @return 每轮操作的时间（单位：毫秒）
	 */
	public Long getCycle() {
		return cycle;
	}

	/**
	 * 设置每轮操作的时间（单位：毫秒）
	 * @param cycle 每轮操作的时间（单位：毫秒）
	 */
	public AssemblyHedge setCycle(Long cycle) {
		this.cycle = cycle;
		return this;
	}
	
	/*****************************************************************
	 * 
	 * 	私有方法定义
	 * 
	 ****************************************************************/
	
	/**
	 * 校验参数是否合法
	 * @return 
	 * 	false 参数校验失败
	 *  true 参数校验成功
	 */
	private Boolean validateParameters(){
		Boolean validateResult = true;
		if(hedgeCurrencyPairs == null || hedgeCurrencyPairs.size() == 0){
			logger.error("各个交易所待操作的交易对不能为空。");
			validateResult = false;
		}
		return validateResult;
	}
	
	/**
	 * 初始化
	 * @return
	 */
	private void init(){
		
		// 构建交易所实例
		if (null == exchanges){
			exchanges = new HashMap<String, Exchange>();
			for (HedgeCurrencyPair hedgeCurrencyPair : hedgeCurrencyPairs){
				String plantform = hedgeCurrencyPair.getPlatform();
				if(exchanges.containsKey(plantform)){
					continue;
				}
				Exchange ex = EndExchangeFactory.newInstance(plantform);
				exchanges.put(plantform, ex);
			}
		}
		
	}
	
	/**
	 * 获取所有对冲交易对的深度信息
	 * @return 对冲交易对的深度信息，若未能成功获取其中某个对冲交易对的信息，则返回 null。
	 */
	private Map<HedgeCurrencyPair, Depth> getHedgeCurrencyPairDepth(){
		final Map<HedgeCurrencyPair, Depth> result = new ConcurrentHashMap<AssemblyHedge.HedgeCurrencyPair, Depth>();
		
		for (final HedgeCurrencyPair hedgeCurrencyPair : hedgeCurrencyPairs){
			final Exchange exchange = exchanges.get(hedgeCurrencyPair.getPlatform());
			new Thread(new Runnable() {
				public void run() {
					Depth depth = exchange.getDepth(hedgeCurrencyPair.getCurrencyPair());
					if(null != depth){
						result.put(hedgeCurrencyPair, depth);
					}
				}
			}).start();
		}
		
		// 前面在并发获取深度信息的过程中，若没有获取到深度，则不将深度信息添加到 result 中
		// 所以这里比较待获取的深度信息的交易对的数量和已经获取到深度信息的交易对的数量就可以知道是否存在某个交易对没有获取到深度信息
		// 若存在没有获取到深度信息的交易对，则返回 null
		if(result.size() != hedgeCurrencyPairs.size()){
			return null;
		}
		
		return result;
	}
	
	
	
	/**
	 * 延时函数
	 * @param ms 延时时间（单位：毫秒）
	 */
	private void delay(Long ms){
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
			logger.error(e);
		}
	}
	
	public void run(Map<String, Object> parameters) {
		
		// 校验参数
		if(!validateParameters()){
			logger.error("校验参数失败！不再执行后续步骤。");
			return;
		}
		
		// 初始化
		init();
		
		// 开始执行策略
		while(true){
			delay(cycle);
			
			Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth = getHedgeCurrencyPairDepth();
			
			
			
			
		}
		
	}

	
	
	public static void main(String[] args){
		Map<String, String> r = new ConcurrentHashMap<String, String>();
		r.put("123", "1");
		System.out.println(r.containsValue("1"));
	}
	
}
