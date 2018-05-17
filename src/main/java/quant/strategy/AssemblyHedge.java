package quant.strategy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import exunion.exchange.Exchange;
import exunion.metaobjects.Account;
import exunion.metaobjects.Account.Balance;
import exunion.metaobjects.Depth;
import exunion.metaobjects.Order;
import exunion.metaobjects.OrderSide;
import exunion.metaobjects.OrderStatus;
import javafx.collections.transformation.SortedList;
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
		
		public HedgeCurrencyPair(String plantform, String currencyPair){
			this.plantfrom = plantform;
			this.currencyPair = currencyPair;
		}
		
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
		public String toString(){
			return "(" + this.plantfrom + ", " + currencyPair + ")";
		}
	}
	
	/*****************************************************************
	 * 
	 * 	全局参数定义
	 * 
	 ****************************************************************/
	
	private static final Logger logger = LogManager.getLogger(AssemblyHedge.class);
	
	private final CommonDao commonDao = new CommonDao();
	
	// 计划组合对冲单
	private final Map<HedgeCurrencyPair, List<AssemblyHedgeOrder>> planAssemblyHedgeOrders = new HashMap<>();
	
	// 已挂出的组合对冲单
	private final Map<HedgeCurrencyPair, List<AssemblyHedgeOrder>> liveAssemblyHedgeOrders = new HashMap<>();
	
	// 各币种账户余额
	private final Map<String, Map<String, Balance>> hedgeCurrencyBalances = new HashMap<String, Map<String,Balance>>(); 
			
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
	public AssemblyHedge setHedgeCurrencyPairs(List<HedgeCurrencyPair> hedgeCurrencyPairs) {
		this.hedgeCurrencyPairs = hedgeCurrencyPairs;
		return this;
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
	public AssemblyHedge setMarketAvailableDuration(Long marketAvailableDuration) {
		this.marketAvailableDuration = marketAvailableDuration;
		return this;
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
	
	/**
	 * 对冲组合单的排序规则
	 * <p> 买单在前，卖单在后
	 * <p> 价格高的在前，价格低的在后
	 * 
	 */
	private Comparator<AssemblyHedgeOrder> hedgeOrderOrderingRule = new Comparator<AssemblyHedgeOrder>() {

		public int compare(AssemblyHedgeOrder o1, AssemblyHedgeOrder o2) {
			if(o1.getOrderSide().equals(o2.getOrderSide())){
				return o2.getOrderPrice().compareTo(o1.getOrderPrice());
			}else{
				return o1.getOrderSide().compareTo(o2.getOrderSide());
			}
		}
	}; 
	
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
			this.exchanges = new HashMap<String, Exchange>();
			
			// 实例化计划对冲单和进行中的对冲单
			this.hedgeCurrencyPairs.stream()
			.forEach(e -> {
				this.planAssemblyHedgeOrders.put(e, new ArrayList<>());
				this.liveAssemblyHedgeOrders.put(e, new ArrayList<>());
			});
			
			// 获取用到的交易所的实例
			this.hedgeCurrencyPairs.stream()
			.filter(e -> !exchanges.containsKey(e.getPlatform()))
			.forEach(e -> exchanges.put(e.getPlatform() , EndExchangeFactory.newInstance(e.getPlatform())));
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
	private Map<HedgeCurrencyPair, AssemblyHedgeOrder> CalculateAssemblyHedgeOrder(Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth){
		Map<HedgeCurrencyPair, AssemblyHedgeOrder> assemblyHedgeOrders = new HashMap<>();
		
		return assemblyHedgeOrders;
	}

	/**
	 * 根据对冲订单组合挂单
	 * @param assemblyHedgeOrders
	 * @return 进行挂单操作之后的组合单，若所有的组合单均挂单失败，则直接清空组合单。
	 */
	private Map<HedgeCurrencyPair, AssemblyHedgeOrder> order(final Map<HedgeCurrencyPair, AssemblyHedgeOrder> assemblyHedgeOrders){
		assemblyHedgeOrders.entrySet().parallelStream().forEach(e -> new Thread(()->order(e.getValue())).start());
		if(0 == assemblyHedgeOrders.entrySet().stream().filter(e->!"PLAN".equals(e.getValue().getOrderStatus())).count()){
			assemblyHedgeOrders.clear();
		}
		return assemblyHedgeOrders;
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
	 * 更新挂单的状态
	 * <p>
	 * 对进行中的订单进行排序，即 <code>liveAssemblyHedgeOrder</code>，排序规则为
	 * 	<ul>
	 * <li> 卖单优先于买单，即 OrderSide desc
	 * <li> 价格由低到高
	 * <p> 排序之后的最终结果为第一个元素为最高价的买单，最后一个元素为最低价的卖单。
	 * 更新订单时只需要两端开始查询更新即可，因为挂单总是优先成交最高价的买单和最低价的卖单。
	 * 特殊情况是跨两个平台，买价高于卖价，这种情况比较少见，这里认为不可能出现此情况。
	 * 
	 * @param liveAssemblyHedgeOrders2 进行中的订单 
	 * @return true - 更新成功； false - 更新失败
	 */
	private Boolean updateHedgeOrders(final Map<HedgeCurrencyPair, List<AssemblyHedgeOrder>> liveAssemblyHedgeOrders){
		Boolean result = false;
		for(Entry<HedgeCurrencyPair, List<AssemblyHedgeOrder>> liveAssemblyHedgeOrder : liveAssemblyHedgeOrders.entrySet()){
			HedgeCurrencyPair currencyPair = liveAssemblyHedgeOrder.getKey();
			List<AssemblyHedgeOrder> assemblyHedgeOrders = liveAssemblyHedgeOrder.getValue();
			Collections.sort(assemblyHedgeOrders, this.hedgeOrderOrderingRule);
			Exchange exchange = this.exchanges.get(currencyPair);
			// 更新买单信息
			for(int i=0; i<assemblyHedgeOrders.size(); i++){
				AssemblyHedgeOrder hedgeOrder = assemblyHedgeOrders.get(i);
				String 
				if(OrderSide.BUY.equals(hedgeOrder.getOrderSide())){
					break;
				}
				
				Order order = exchange.getOrder(hedgeOrder.getCurrency(), hedgeOrder.getOrderId());
				if(null == order){
					logger.error("获取订单(plantform=,{}, currency={}, orderId={})信息时失败。", hedgeOrder.getPlantfrom(), hedgeOrder.getCurrency(), hedgeOrder.getOrderId());
					return false;
				}
				
				
				
			}
			
			// 更新卖单信息
			
			
		}
		return result;
	}
	
	/**
	 * 更新各个币种的可用余额
	 * @return
	 */
	private Boolean updateBalances(){
		logger.debug("准备更新账户余额。");
		this.hedgeCurrencyBalances.clear();
		for(HedgeCurrencyPair hedgeCurrencyPair : this.hedgeCurrencyPairs){
			
			String plantform = hedgeCurrencyPair.getPlatform();
			// 此轮已经获取到了该平台各币种的余额
			if(this.hedgeCurrencyBalances.containsKey(plantform)){
				continue;
			}
			Exchange exchange = exchanges.get(plantform);
			Account account = exchange.getAccount();
			if(account == null){
				logger.info("更新账户信息失败。");
				return false;
			}
			this.hedgeCurrencyBalances.put(plantform, account.getBalances());
		}
		logger.debug("账户余额更新完成。");
		return true;
	}
	
	/**
	 * 根据深度信息下计划订单。
	 * <p> 根据深度信息，若买一价大于计划单的卖价，则下卖单；若卖一价小于计划单的买价，则下买单。
	 * <p> 一次做多下一个买单，最多下一个卖单。
	 * @param depths 深度信息
	 * @param planAssemblyHedgeOrders 经过排序的计划对冲单
	 * @param liveAssemblyHedgeOrders 进行中的计划对冲单
	 * 
	 */
	private Boolean placePlanHedgeOrders(final Map<HedgeCurrencyPair, Depth> depths,
			final Map<HedgeCurrencyPair, List<AssemblyHedgeOrder>> planAssemblyHedgeOrders, 
			final Map<HedgeCurrencyPair, List<AssemblyHedgeOrder>> liveAssemblyHedgeOrders){
		logger.debug("准备下计划单 ...");
		boolean isPlacedPlanOrders = false; // 是否下了计划订单标志
		
		for(HedgeCurrencyPair currencyPair : this.hedgeCurrencyPairs){
			Depth depth = depths.get(currencyPair);
			BigDecimal buy1Price = depth.getBids().get(0).getPrice();
			BigDecimal sell1Price = depth.getAsks().get(0).getPrice();
			List<AssemblyHedgeOrder> planHedgeOrders = planAssemblyHedgeOrders.get(currencyPair);
			List<AssemblyHedgeOrder> liveHedgeOrders = liveAssemblyHedgeOrders.get(currencyPair);
			
			int size = planHedgeOrders.size();
			
			AssemblyHedgeOrder maxPriceBuyOrder = null;
			AssemblyHedgeOrder minPriceSellOrder = null;
			
			// 尝试获取最高价的计划买单
			if(size>0 && OrderSide.BUY.equals(planHedgeOrders.get(0).getOrderSide())){
				maxPriceBuyOrder = planHedgeOrders.get(0);
			}
			
			// 尝试获取最低价的计划卖单
			if(size>0 && OrderSide.SELL.equals(planHedgeOrders.get(size - 1).getOrderSide())){
				minPriceSellOrder = planHedgeOrders.get(size - 1);
			}
			
			// 最高价的计划买单的价格高于卖一价，符合下单条件。
			if(null != maxPriceBuyOrder && maxPriceBuyOrder.getOrderPrice().compareTo(sell1Price)>0){
				this.order(maxPriceBuyOrder);
				// 挂单的状态发生了变化，表示下单成功了。
				if(!"PLAN".equals(maxPriceBuyOrder.getOrderStatus())){
					logger.debug("{}最高价计划买单价格: {}, 卖一价: {},计划买单下单成功。", currencyPair, maxPriceBuyOrder.getOrderPrice(), sell1Price);
					new Thread(()->commonDao.saveOrUpdate(planHedgeOrders.get(0))).start();
					liveHedgeOrders.add(maxPriceBuyOrder);
					planHedgeOrders.remove(0);
					isPlacedPlanOrders = true;
				}
			}
			
			//最低价的计划卖单的价格低于买一价，符合下单条件。 这里使用 else if 是由于此条件与上面的条件不可能同时满足。
			else if(null != minPriceSellOrder && minPriceSellOrder.getOrderPrice().compareTo(buy1Price) < 0){
				this.order(minPriceSellOrder);
				if(!"PLAN".equals(minPriceSellOrder.getOrderStatus())){
					logger.debug("{}最低价计划卖单价格： {}, 买一价： {},计划卖单下单成功。",currencyPair, minPriceSellOrder.getOrderPrice(), buy1Price);
					new Thread(()->commonDao.saveOrUpdate(planHedgeOrders.get(size - 1))).start();
					liveHedgeOrders.add(minPriceSellOrder);
					planHedgeOrders.remove(size - 1);
					isPlacedPlanOrders = true;
				}
			}
		}
		
		logger.debug("下计划单完成。");
		return isPlacedPlanOrders;
	}
	
	/**
	 * 下对冲订单。
	 * @return
	 */
	private void placeHedgeOrders(final Map<HedgeCurrencyPair, Depth> currencyPairDepth,
			final Map<HedgeCurrencyPair, List<AssemblyHedgeOrder>> planHedgeOrders, 
			final Map<HedgeCurrencyPair, List<AssemblyHedgeOrder>> liveHedgeOrders){
		
		// 计算组合单
		Map<HedgeCurrencyPair, AssemblyHedgeOrder> assemblyHedgeOrders = this.CalculateAssemblyHedgeOrder(currencyPairDepth);
		if(0 == assemblyHedgeOrders.size()){
			logger.info("没有组合对冲交易机会。");
		}else{
			// 挂组合单
			Map<HedgeCurrencyPair, AssemblyHedgeOrder> hedgeOrders = order(assemblyHedgeOrders);
			for(Entry<HedgeCurrencyPair, AssemblyHedgeOrder> hedgeOrder : hedgeOrders.entrySet()){
				if("PLAN".equals(hedgeOrder.getValue().getOrderStatus())){
					planHedgeOrders.get(hedgeOrder.getKey()).add(hedgeOrder.getValue());
				}else{
					liveHedgeOrders.get(hedgeOrder.getKey()).add(hedgeOrder.getValue());;
				}
			}
			hedgeOrders.entrySet().parallelStream().forEach(e->commonDao.saveOrUpdate(e.getValue()));
		}
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
			
			// 更新挂单状态
			if(!updateHedgeOrders(this.liveAssemblyHedgeOrders)){
				logger.error("更新挂单信息失败，将在 {} 秒后进行下一轮操作。", failedSleepTime / 1000);
				delay(failedSleepTime);
				continue;
			}
			
			// 获取深度信息
			Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth = getHedgeCurrencyPairDepth();
			if(null == hedgeCurrencyPairDepth){ // 获取深度信息失败
				logger.error("获取深度信息失败, 将在 {} 秒后进行下一轮操作。", failedSleepTime / 1000);
				delay(failedSleepTime);
				continue;
			}
			
			// 优先下计划订单
			Boolean isPlacedPlanOrders = this.placePlanHedgeOrders(hedgeCurrencyPairDepth, this.planAssemblyHedgeOrders, this.liveAssemblyHedgeOrders); 
			// 已经下了计划订单，考虑到对市场的影响，不再进行后续操作，直接进入下一轮。
			if(isPlacedPlanOrders){
				continue;
			}
			
			// 前面操作都 OK 了，开始下组合对冲单
			this.placeHedgeOrders(hedgeCurrencyPairDepth, this.planAssemblyHedgeOrders, this.liveAssemblyHedgeOrders);
			
		}
		
	}

	
	
	public static void main(String[] args){
		
		List<HedgeCurrencyPair> hedgeCurrencyPairs = new ArrayList<>();
		hedgeCurrencyPairs.add(new HedgeCurrencyPair("exx.com", "HSR_QC"));
		hedgeCurrencyPairs.add(new HedgeCurrencyPair("zb.com", "HSR_QC"));
		
		
		new AssemblyHedge()
		.setCycle(5000L)
		.setFailedSleepTime(30000L)
		.setHedgeCurrencyPairs(hedgeCurrencyPairs)
		.setMarketAvailableDuration(2000L)
		.setMaxPlanOrderNumber(10)
		.run(null);
	}
	
}
