package quant.strategy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import exunion.exchange.Exchange;
import exunion.metaobjects.Depth;
import exunion.metaobjects.Order;
import exunion.metaobjects.OrderStatus;
import quant.dao.CommonDao;
import quant.entity.AssemblyHedgeOrder;
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
	
	private final CommonDao commonDao = new CommonDao();
	
	// 组合对冲币种对，即参与对冲组合的币种对，属性包括平台，币种对
	private List<HedgeCurrencyPair> hedgeCurrencyPairs = null;
	
	/**
	 * 获取组合对冲币种对
	 * <p>
	 * 组合对冲币种对，即参与对冲组合的币种对，属性包括平台，币种对
	 * @return 组合对冲币种对
	 */
	public List<HedgeCurrencyPair> getHedgeCurrencyPairs() {
		return hedgeCurrencyPairs;
	}

	/**
	 * 获取组合对冲币种对
	 * <p>
	 * 组合对冲币种对，即参与对冲组合的币种对，属性包括平台，币种对
	 * @param hedgeCurrencyPairs 组合对冲币种对
	 */
	public void setHedgeCurrencyPairs(List<HedgeCurrencyPair> hedgeCurrencyPairs) {
		this.hedgeCurrencyPairs = hedgeCurrencyPairs;
	}

	// 交易所实例
	private Map<String, Exchange> exchanges = null;
	
	/**
	 * @return 当前策略实例使用到的交易所实例
	 */
	public Map<String, Exchange> getExchanges() {
		return exchanges;
	}
	
	//API操作失败休息时间， （单位：毫秒），默认 1000ms
	private Long failedSleepTime = 1000L;
	
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
	
	//操作周期, 默认10毫秒
	private Long cycle = 10L;
	
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
	
	
	/**
	 *  市场有效时间(单位：毫秒)，默认值 500，表示市场行情有效时间，
	 *  即在获取行情数据时，交易所必须在此时间之内返回行情数据，否则认为获取到的数据无效
	 */
	private Long marketAvailableDuration = 500L;
	
	/**
	 * 获取市场有效时间
	 * <p>
	 * 市场有效时间(单位：毫秒)，默认值 500，表示市场行情有效时间，
	 * 即在获取行情数据时，交易所必须在此时间之内返回行情数据，否则认为获取到的数据无效
	 * @return 市场有效时间
	 */
	public Long getMarketAvailableDuration() {
		return marketAvailableDuration;
	}

	/**
	 * 设置市场有效时间
	 * <p>
	 * 市场有效时间(单位：毫秒)，默认值 500，表示市场行情有效时间，
	 * 即在获取行情数据时，交易所必须在此时间之内返回行情数据，否则认为获取到的数据无效
	 * @param marketAvailableDuration 市场有效时间，默认值 500
	 */
	public void setMarketAvailableDuration(Long marketAvailableDuration) {
		this.marketAvailableDuration = marketAvailableDuration;
	}

	
	// 最大计划订单数
	private Integer maxPlanOrderNumber = 5;
	
	/**
	 * 默认值为 5， 当达到最大订单数时，不再创建新的对冲交易订单组合
	 * @return 最大计划订单数
	 */
	public Integer getMaxPlanOrderNumber() {
		return maxPlanOrderNumber;
	}

	/**
	 * 
	 * 默认值为 5， 当达到最大订单数时，不再创建新的对冲交易订单数
	 * @param maxPlanOrderNumber 最大计划订单数
	 */
	public AssemblyHedge setMaxPlanOrderNumber(Integer maxPlanOrderNumber) {
		this.maxPlanOrderNumber = maxPlanOrderNumber;
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
	private Boolean init(){
		
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
		
		return true;
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
		
		// 判断是否获取到各币种的深度信息
		// 每10ms查询一次，直到达到最大次数或者该获取的行情信息都获取到了则退出循环
		Long maxCycleTimes = (marketAvailableDuration - (marketAvailableDuration % 10)) / 10;
		Long i = 0L;
		Boolean isGetAllDepth = false;
		while(i++ < maxCycleTimes){
			delay(10L);
			if(result.size() == hedgeCurrencyPairs.size()){
				isGetAllDepth = true;
				break;
			}
		}
		
		// 获取到了所有的行情信息
		if(isGetAllDepth){
			return result;			
		}
		return null;
	}
	
	/**
	 * 根据对冲交易对的深度信息计算组合挂单。
	 * @param hedgeCurrencyPairDepth 对冲币种对的深度信息
	 * @return 计算得到组合对冲订单，若无对冲机会，则返回 null
	 */
	private List<AssemblyHedgeOrder> CalculateAssemblyHedgeOrder(Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth){
		List<AssemblyHedgeOrder> assemblyHedgeOrders = new ArrayList<AssemblyHedgeOrder>();
		
		return assemblyHedgeOrders;
	}
	
	/**
	 * 根据对冲订单组合挂单
	 * @param assemblyHedgeOrders 对冲组合订单
	 * @return 订单信息
	 */
	private List<AssemblyHedgeOrder> order(List<AssemblyHedgeOrder> assemblyHedgeOrders){
		final List<AssemblyHedgeOrder> hedgeOrders = Collections.synchronizedList(new ArrayList<AssemblyHedgeOrder>());
		
		// 并发下订单
		for(final AssemblyHedgeOrder assemblyHedgeOrder: assemblyHedgeOrders){
			
			new Thread(new Runnable() {
				public void run() {
					AssemblyHedgeOrder hedgeOrder = order(assemblyHedgeOrder);
					hedgeOrders.add(hedgeOrder);
				}
			}).start();
			
		}

		// 等待下单完成
		while(assemblyHedgeOrders.size()!=hedgeOrders.size()){
			delay(10L);
		}
		
		// 判断是否所有的订单都下单失败了，即所有订单的状态都是 PLAN
		// 若对冲组合中存在下单成功的订单，则失败的订单的状态变为 PLAN，日后伺机下单
		Boolean allOrdersArePlan = true;
		for(AssemblyHedgeOrder hedgeOrder : hedgeOrders){
			if(!"PLAN".equals(hedgeOrder.getOrderStatus())){
				allOrdersArePlan = false;				
				break;
			}
		}
		// 若所有的订单都挂单失败了，则表示此轮对冲组合无效，则直接清空此轮的订单
		if(allOrdersArePlan){
			hedgeOrders.clear();
		}
		
		return hedgeOrders;
	}
	
	/**
	 * 根据组合对冲订单的成员信息下订单
	 * @param assemblyHedgeOrder 组合对冲订单对
	 * @return 订单信息
	 */
	private AssemblyHedgeOrder order(final AssemblyHedgeOrder assemblyHedgeOrder){
		String exchangeName = assemblyHedgeOrder.getPlantfrom();
		Exchange exchange = exchanges.get(exchangeName);
		String side = assemblyHedgeOrder.getOrderSide();
		String currency = assemblyHedgeOrder.getCurrency();
		BigDecimal quantity = assemblyHedgeOrder.getOrderQuantity();
		BigDecimal price = assemblyHedgeOrder.getOrderPrice();
		Order order = exchange.order(side, currency, quantity, price);
		if(null != order){
			assemblyHedgeOrder.setOrderId(order.getOrderId());
			assemblyHedgeOrder.setOrderStatus(OrderStatus.NEW);
		}else{
			assemblyHedgeOrder.setOrderStatus("PLAN");
		}
		return assemblyHedgeOrder;
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
		logger.info("参数校验完成 ...");
		
		// 初始化
		if(!init()){
			logger.error("初始化失败！不再执行后续步骤。");
			return;
		}
		logger.info("初始化完成 ...");
		
		// 开始执行策略
		while(true){
			logger.debug("新一轮。");
			delay(cycle);
			
			Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth = getHedgeCurrencyPairDepth();
			if(null == hedgeCurrencyPairDepth){ // 获取深度信息失败
				logger.error("获取深度信息失败, 将在 {} 秒后进行下一轮操作。", failedSleepTime / 60);
				delay(failedSleepTime);
				continue;
			}
			
			// 计算组合单
			List<AssemblyHedgeOrder> assemblyHedgeOrders = this.CalculateAssemblyHedgeOrder(hedgeCurrencyPairDepth);
			if(0 == assemblyHedgeOrders.size()){
				logger.info("没有组合对冲交易机会。");
			}else{
				// 挂组合单
				List<AssemblyHedgeOrder> hedgeOrders = order(assemblyHedgeOrders);
				this.commonDao.saveOrUpdate(hedgeOrders);
			}
			
		}
		
	}

	
	
	public static void main(String[] args){
		
		List<String> l = Collections.synchronizedList(new ArrayList<String>());
		l.add(null);
		l.add(null);
		l.add("1");
		System.out.println(l.contains(null));
		
	}
	
}
