package quant.strategy;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
import exunion.metaobjects.Depth.PriceQuotation;
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
	
	/**
	 * 对冲元素类
	 * <p>
	 * 这里认为一组对冲实际上由一个环构成，环上有节点，一个节点称为一个 #HedgeElement
	 * @author robothy
	 *
	 */
	private static class HedgeElement{
		
		private String exchange = null;
		
		private String orderType = null;
		
		private List<PriceQuotation> depth = null;
		
		
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
	
	/**
	 * 交易手续费费率，姑且认为不同交易所的费率相同
	 */
	private BigDecimal feeRate = new BigDecimal("0.0021");
	
	/**
	 * 这里认为不同交易所的费率相同
	 * @return 交易手续费费率
	 */
	public BigDecimal getFeeRate(){
		return feeRate;
	}
	
	/**
	 * 这里认为不同交易所的费率相同，这里为了避免除法产生尾差造成损失，手续费率可以稍微调高一点。
	 * @param feeRate 交易手续费费率， 默认 0.0021
	 * @return
	 */
	public AssemblyHedge setFeeRate(BigDecimal feeRate){
		this.feeRate = feeRate;
		return this;
	}
	
	/**
	 * 价格精度，小数点后的位数
	 * 
	 */
	private Integer priceScale = null; 
	
	/**
	 * 设置价格精度，精度用整数表示，例如：2表示保留小数点后2位
	 * @param priceScale 价格精度
	 * @return
	 */
	public AssemblyHedge setPriceScale(Integer priceScale){
		this.priceScale = priceScale;
		return this;
	}
	
	/**
	 * 获取价格精度，精度用整数表示，例如：2表示保留小数点后2位
	 * @return 价格精度
	 */
	public Integer  getPriceScale(){
		return this.priceScale;
	}
	
	/**
	 * 量精度，精度用整数表示，例如：2表示保留小数点后2位
	 */
	private Integer quantityScale = null;
	
	/**
	 * 量精度，精度用整数表示，例如：2表示保留小数点后2位
	 * @param quantityScale 量精度
	 * @return
	 */
	public AssemblyHedge setQuantityScale(Integer quantityScale){
		this.quantityScale = quantityScale;
		return this;
	}
	
	/**
	 * 量精度，精度用整数表示，例如：2表示保留小数点后2位
	 * @return 量精度
	 */
	public Integer getQuantityScale(){
		return this.quantityScale;
	}
	
	/**
	 * 常数 1
	 */
	private static final BigDecimal ONE = new BigDecimal("1");
	
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
		logger.debug("开始构建交易所实例 ...");
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
		logger.debug("交易所实例构建完成。");
		return true;
	}
	
	/**
	 * 获取所有对冲交易对的深度信息
	 * @return 对冲交易对的深度信息，若未能成功获取其中某个对冲交易对的信息，则返回 null。
	 */
	private Map<HedgeCurrencyPair, Depth> getHedgeCurrencyPairDepth(){
		
		logger.debug("开始获取深度信息 ...");
		Map<HedgeCurrencyPair, Depth> result = new ConcurrentHashMap<AssemblyHedge.HedgeCurrencyPair, Depth>();
		
		Long begin = System.currentTimeMillis();
		this.hedgeCurrencyPairs.parallelStream().forEach(hedgeCurrencyPair -> {
			final Exchange exchange = exchanges.get(hedgeCurrencyPair.getPlatform());
			Depth depth = exchange.getDepth(hedgeCurrencyPair.getCurrencyPair());
			if(depth != null){
				result.put(hedgeCurrencyPair, depth);
			}
		} );
		Long end = System.currentTimeMillis();
		Long duration = end - begin;
		
		logger.debug(result);
		
		if(duration > this.marketAvailableDuration){
			logger.warn("获取深度信息的时间{}ms超过了市场有效时间{}ms。", duration, this.marketAvailableDuration);
			return null;
		}
		
		if(this.hedgeCurrencyPairs.size() != result.size()){
			for(HedgeCurrencyPair hedgeCurrencyPair : this.hedgeCurrencyPairs){
				if(result.containsKey(hedgeCurrencyPair)){
					continue;
				}
				logger.error("未能获取{}的深度信息。", hedgeCurrencyPair);	
				return null;
			}
		}
		logger.debug("深度信息获取完成 ...");
		return result;
	}
	
	/**
	 * 根据对冲交易对的深度信息计算组合挂单。
	 * @param hedgeCurrencyPairDepth 对冲币种对的深度信息
	 * @return 计算得到组合对冲订单，若无对冲机会，则返回 null
	 */
	private Map<HedgeCurrencyPair, AssemblyHedgeOrder> CalculateAssemblyHedgeOrder(Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth){
		Map<HedgeCurrencyPair, AssemblyHedgeOrder> assemblyHedgeOrders = new HashMap<>();
		
		// 双币种搬砖套利
		if(hedgeCurrencyPairs.size() == 2){
			return CalculateDoubleAssemblyHedgeOrder(this.hedgeCurrencyPairs, hedgeCurrencyPairDepth, this.hedgeCurrencyBalances);
		}
		
		
		return assemblyHedgeOrders;
	}
	
	/**
	 * 双币种对套利，两个币种必须相同
	 * @param hedgeCurrencyPairDepth
	 * @param hedgeCurrencyBalances
	 * @return
	 */
	private Map<HedgeCurrencyPair, AssemblyHedgeOrder> CalculateDoubleAssemblyHedgeOrder(List<HedgeCurrencyPair> hedgeCurrencyPairs,Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth, Map<String, Map<String, Balance>> hedgeCurrencyBalances){
		
		Map<HedgeCurrencyPair, AssemblyHedgeOrder> hedgeOrders = new HashMap<>();
		
		HedgeCurrencyPair mainExchangeCurrencyPair = hedgeCurrencyPairs.get(0);
		HedgeCurrencyPair viceExchangeCurrencyPair = hedgeCurrencyPairs.get(1);
		
		String mainExchange = mainExchangeCurrencyPair.getPlatform(); // 主交易所名称
		String viceExchange = viceExchangeCurrencyPair.getPlatform(); // 副交易所名称
		
		String mainCurrencyPair = mainExchangeCurrencyPair.getCurrencyPair(); // 主交易所币种对
		String viceCurrencyPair = viceExchangeCurrencyPair.getCurrencyPair(); // 副交易所币种对
		
		String[] currencies = mainCurrencyPair.split("_");
		String mainExchangeBaseCurrency = currencies[0]; // 主交易所基础货币
		String mainExchangeQuoteCurrency = currencies[1]; // 主交易所计价货币
		
		currencies = viceCurrencyPair.split("_");
		String viceExchangeBaseCurrency = currencies[0]; // 副交易所基础货币
		String viceExchangeQuoteCurrency = currencies[1]; // 副交易所计价货币
		
		List<PriceQuotation> mainExchangeCurrencyPairAsks = hedgeCurrencyPairDepth.get(mainExchangeCurrencyPair).getAsks();
		List<PriceQuotation> mainExchangeCurrencyPairBids = hedgeCurrencyPairDepth.get(mainExchangeCurrencyPair).getBids();
		List<PriceQuotation> viceExchangeCurrencyPairAsks = hedgeCurrencyPairDepth.get(viceExchangeCurrencyPair).getAsks();
		List<PriceQuotation> viceExchangeCurrencyPairBids = hedgeCurrencyPairDepth.get(viceExchangeCurrencyPair).getBids();
		
		// 主交易所与副交易所的币种对相同
		if(mainExchangeBaseCurrency.equals(viceExchangeBaseCurrency) && mainExchangeQuoteCurrency.equals(viceExchangeQuoteCurrency)){
			
			BigDecimal mainCurrencyPairBuy1Price = mainExchangeCurrencyPairBids.get(0).getPrice();
			BigDecimal mainCurrencyPairSell1Price = mainExchangeCurrencyPairAsks.get(0).getPrice();

			BigDecimal viceCurrencyPairBuy1Price = viceExchangeCurrencyPairBids.get(0).getPrice();
			BigDecimal viceCurrencyPairSell1Price = viceExchangeCurrencyPairAsks.get(0).getPrice();
			
			// 主交易所的买一价大于副交易所的卖一价，存在套利空间
			if(mainCurrencyPairBuy1Price.compareTo(viceCurrencyPairSell1Price) > 0){
				logger.debug("存在搬砖套利可能的。");
				// 既然主交易所的买一价大于副交易所的卖一价，那么应该在主交易所卖出，副交易所买入
				BigDecimal mainCurrencyPairSellQuantity = new BigDecimal("0");
				BigDecimal mainCurrencyPairSellPrice = new BigDecimal("0");
				BigDecimal viceCurrencyPairBuyQuantity = new BigDecimal("0");
				BigDecimal viceCurrencyPairBuyPrice = new BigDecimal("0");
				while(mainExchangeCurrencyPairBids.size() > 0 && viceExchangeCurrencyPairAsks.size() > 0){
					BigDecimal hedgeBuyQuantity = null;
					BigDecimal hedgeSellQuantity = null;
					BigDecimal hedgeBuyPrice = null;
					BigDecimal hedgeSellPrice = null;
					
					
					
					if(mainExchangeCurrencyPairBids.get(0).getQuantity().compareTo(viceExchangeCurrencyPairAsks.get(0).getQuantity()) > 0){
						// 对冲买单的数量 = 少端 ÷ (1 - 费率)
						// 这里避免手续费单边消耗基础货币, 对冲的量取决于量少的那一端
						hedgeSellQuantity = viceExchangeCurrencyPairAsks.get(0).getQuantity();
						hedgeSellPrice = mainExchangeCurrencyPairBids.get(0).getPrice();
						hedgeBuyQuantity = hedgeSellQuantity.divide(ONE.subtract(this.feeRate), quantityScale + 3, RoundingMode.DOWN);
						hedgeBuyPrice = viceExchangeCurrencyPairAsks.get(0).getPrice();
						
						//移除对冲掉的部分s
						viceExchangeCurrencyPairAsks.remove(0);
						mainExchangeCurrencyPairBids.get(0).setQuantity(mainExchangeCurrencyPairBids.get(0).getQuantity().subtract(hedgeSellQuantity));
					}else{
						hedgeSellQuantity = mainExchangeCurrencyPairAsks.get(0).getQuantity();
						hedgeSellPrice = mainExchangeCurrencyPairBids.get(0).getPrice();
						hedgeBuyQuantity = hedgeSellQuantity.divide(ONE.subtract(this.feeRate), quantityScale + 3, RoundingMode.DOWN);
						hedgeBuyPrice = viceExchangeCurrencyPairAsks.get(0).getPrice();
						
						mainExchangeCurrencyPairBids.remove(0);
						viceExchangeCurrencyPairAsks.get(0).setQuantity(viceExchangeCurrencyPairAsks.get(0).getQuantity().subtract(hedgeBuyQuantity));
					}
					
					// 检查是否亏本
					BigDecimal comsumedQuoteCurrency = hedgeBuyQuantity.multiply(hedgeBuyPrice); // 消耗的计价货币(买入消耗)
					//BigDecimal earnedBaseCurrency = hedgeBuyQuantity;	// 得到的基础货币( 买入得到)
					
					//BigDecimal comsumedBaseCurrency = hedgeSellQuantity; // 消耗的基础货币（卖出消耗） 
					BigDecimal earnedQuoteCurrency = hedgeSellQuantity.multiply(hedgeSellPrice).multiply(ONE.subtract(this.feeRate)); // 得到的计价货币（卖出得到）
					
					// 这里不检查基础货币的量了，计算的时候得到的基础货币的量几乎等于计价货币的量，
					// 但由于尾差，得到的基础的量总会比消耗的基础货币的量小一点点，但这一点点可以忽略不计，可以通过稍微调大手续费率进行补偿。
					if(earnedQuoteCurrency.compareTo(comsumedQuoteCurrency) > 0){ // 得到的计价货币比消耗的计价货币多，对冲能套利
						mainCurrencyPairSellQuantity.add(hedgeSellQuantity);
						mainCurrencyPairSellPrice = hedgeSellPrice;
						viceCurrencyPairBuyQuantity.add(hedgeBuyQuantity);
						viceCurrencyPairBuyPrice = hedgeBuyPrice;
					}else{ // 没有对冲的机会了，直接跳出循环
						break;
					}
				}
				logger.debug("找到对冲套利机会，  {} {} 买入 {} {}, {} {} 卖出 {} {}", viceExchange, viceCurrencyPairBuyPrice, viceCurrencyPairBuyQuantity,viceCurrencyPair, mainExchange, mainCurrencyPairSellPrice, mainCurrencyPairSellQuantity, mainCurrencyPair);
			}
			
			// 主交易所的卖一价小于副交易所的买一价，存在套利空间
			else if(mainCurrencyPairSell1Price.compareTo(viceCurrencyPairBuy1Price) < 0){
				logger.debug("存在搬砖套利可能的。");
				// 既然主交易所的买一价小于副交易所的卖一价，那么应该在主交易所买入，副交易所卖出
				BigDecimal mainCurrencyPairBuyQuantity = new BigDecimal("0");
				BigDecimal mainCurrencyPairBuyPrice = new BigDecimal("0");
				BigDecimal viceCurrencyPairSellQuantity = new BigDecimal("0");
				BigDecimal viceCurrencyPairSellPrice = new BigDecimal("0");
				while(mainExchangeCurrencyPairAsks.size() > 0 && viceExchangeCurrencyPairBids.size() > 0){
					BigDecimal hedgeBuyQuantity = null;
					BigDecimal hedgeBuyPrice = null;
					BigDecimal hedgeSellQuantity = null;
					BigDecimal hedgeSellPrice = null;
					
					
					
					if(mainExchangeCurrencyPairAsks.get(0).getQuantity().compareTo(viceExchangeCurrencyPairBids.get(0).getQuantity()) > 0){
						hedgeSellQuantity = viceExchangeCurrencyPairBids.get(0).getQuantity();
						hedgeSellPrice = viceExchangeCurrencyPairBids.get(0).getPrice();
						hedgeBuyQuantity = hedgeSellQuantity.divide(ONE.subtract(this.feeRate), quantityScale + 3, RoundingMode.DOWN);
						hedgeBuyPrice = mainExchangeCurrencyPairAsks.get(0).getPrice();
						
						viceExchangeCurrencyPairBids.remove(0);
						mainExchangeCurrencyPairAsks.get(0).setQuantity(mainExchangeCurrencyPairAsks.get(0).getQuantity().subtract(hedgeBuyQuantity));
					}else{
						hedgeSellQuantity = mainExchangeCurrencyPairAsks.get(0).getQuantity();
						hedgeSellPrice = viceExchangeCurrencyPairBids.get(0).getPrice();
						hedgeBuyQuantity = hedgeSellQuantity.divide(ONE.subtract(this.feeRate), quantityScale + 3, RoundingMode.DOWN);
						hedgeBuyPrice = mainExchangeCurrencyPairAsks.get(0).getPrice();
						
						mainExchangeCurrencyPairAsks.remove(0);
						viceExchangeCurrencyPairBids.get(0).setQuantity(viceExchangeCurrencyPairBids.get(0).getQuantity().subtract(hedgeSellQuantity));
					}
					
					// 检查是否亏本
					BigDecimal comsumedQuoteCurrency = hedgeBuyQuantity.multiply(hedgeBuyPrice);
					BigDecimal earnedQuoteCurrency = hedgeSellQuantity.multiply(hedgeSellPrice).multiply(ONE.subtract(this.feeRate));
					if(earnedQuoteCurrency.compareTo(comsumedQuoteCurrency) > 0){
						mainCurrencyPairBuyQuantity = mainCurrencyPairBuyQuantity.add(hedgeBuyQuantity);
						mainCurrencyPairBuyPrice = hedgeBuyPrice;						
						viceCurrencyPairSellPrice = hedgeSellPrice;
						viceCurrencyPairSellQuantity = viceCurrencyPairSellQuantity.add(hedgeSellQuantity);
					}else{
						break;
					}
					
				}
				logger.debug("找到对冲套利机会，  {} {}买入{} {}, {} {}卖出{} {}", mainExchange, mainCurrencyPairBuyPrice, mainCurrencyPairBuyQuantity,mainCurrencyPair, viceExchange, viceCurrencyPairSellPrice,viceCurrencyPairSellQuantity, viceCurrencyPair);
			}
		}
		// 主交易所与副交易所的币种相反
		else if(mainExchangeBaseCurrency.equals(viceExchangeQuoteCurrency) && mainExchangeQuoteCurrency.equals(viceExchangeBaseCurrency)){
			logger.info("此策略暂不支持主交易所与副交易所币种相反的情况。");
		}else{
			logger.error("币种对{}的{}与{}的{}不能进行对冲套利。", mainExchange, mainCurrencyPair, viceExchange, viceCurrencyPair);
		}
		
		return hedgeOrders;
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
		logger.debug("准备更新挂单信息 ...");
		for(Entry<HedgeCurrencyPair, List<AssemblyHedgeOrder>> liveAssemblyHedgeOrder : liveAssemblyHedgeOrders.entrySet()){
			HedgeCurrencyPair currencyPair = liveAssemblyHedgeOrder.getKey();
			List<AssemblyHedgeOrder> assemblyHedgeOrders = liveAssemblyHedgeOrder.getValue();
			Collections.sort(assemblyHedgeOrders, this.hedgeOrderOrderingRule);
			Exchange exchange = this.exchanges.get(currencyPair);
			// 更新买单信息
			for(int i=0; i<assemblyHedgeOrders.size(); i++){
				AssemblyHedgeOrder hedgeOrder = assemblyHedgeOrders.get(i);
				// 第一个订单不是买单，直接跳出
				if(!OrderSide.BUY.equals(hedgeOrder.getOrderSide())){
					break;
				}
				
				Order order = exchange.getOrder(hedgeOrder.getCurrency(), hedgeOrder.getOrderId());
				if(null == order){
					logger.error("获取订单(plantform=,{}, currency={}, orderId={})信息时失败。", hedgeOrder.getPlantfrom(), hedgeOrder.getCurrency(), hedgeOrder.getOrderId());
					return false;
				}
				
				String previousStatus = hedgeOrder.getOrderStatus();
				// 订单状态发生了改变
				if(!previousStatus.equals(order.getStatus()) || OrderStatus.FILLED.equals(order.getStatus())){
					hedgeOrder.setOrderStatus(order.getStatus());
					hedgeOrder.setTransPrice(order.getPrice());
					commonDao.saveOrUpdate(hedgeOrder);
				}
				// 若价格高的买单状态都发生改变，那价格低的买单肯定也未发生改变，直接跳出循环，不再检查价格低的买单
				else{
					break;
				}
			}
			
			// 更新卖单信息
			for(int i=assemblyHedgeOrders.size() - 1; i>0; i++){
				AssemblyHedgeOrder hedgeOrder = assemblyHedgeOrders.get(i);
				
				// 最后一个单不是卖单，直接跳出
				if(!OrderSide.SELL.equals(hedgeOrder.getOrderSide())){
					break;
				}
				
				Order order = exchange.getOrder(hedgeOrder.getCurrency(), hedgeOrder.getOrderId());
				if(null == order){
					logger.error("获取订单(plantform=,{}, currency={}, orderId={})信息时失败。", hedgeOrder.getPlantfrom(), hedgeOrder.getCurrency(), hedgeOrder.getOrderId());
					return false;
				}
				
				String previoursStatus = hedgeOrder.getOrderStatus();
				if(!previoursStatus.equals(order.getStatus()) || OrderStatus.FILLED.equals(order.getStatus())){
					hedgeOrder.setOrderStatus(order.getStatus());
					hedgeOrder.setTransPrice(order.getPrice());
					commonDao.saveOrUpdate(hedgeOrder);
				}else{
					break;
				}
			}
		}
		logger.debug("挂单信息更新完成。");
		return true;
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
		logger.debug("准备挂对冲单 ...");
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
		logger.debug("对冲单挂单完成。");
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
		hedgeCurrencyPairs.add(new HedgeCurrencyPair("gate.io", "EOS_BTC"));
		hedgeCurrencyPairs.add(new HedgeCurrencyPair("bit-z.com", "EOS_BTC"));
		
		new AssemblyHedge()
		.setCycle(5000L)
		.setFailedSleepTime(30000L)
		.setHedgeCurrencyPairs(hedgeCurrencyPairs)
		.setMarketAvailableDuration(3000L)
		.setMaxPlanOrderNumber(10)
		.setFeeRate(new BigDecimal("0.002"))
		.setQuantityScale(4)
		.setPriceScale(4)
		.run(null);
		
		
		//EndExchangeFactory.newInstance("bit-z.com").order("SELL", "ETH_BTC", new BigDecimal("1"), new BigDecimal("10000"));
		
		
	}
	
}
