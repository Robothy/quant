package quant.candidate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
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
import quant.dao.CommonDao;
import quant.entity.CandidateAssemblyHedgeOrder;
import quant.exchange.EndExchangeFactory;
import quant.strategy.Strategy;

public class CandidateAssemblyHedge implements Strategy {
	
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
	
	private static final Logger logger = LogManager.getLogger("quant");
	
	private final CommonDao commonDao = new CommonDao();
	
	// 计划组合对冲单
	private final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> planCandidateAssemblyHedgeOrders = new HashMap<>();
	
	// 已挂出的组合对冲单
	private final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> liveCandidateAssemblyHedgeOrders = new HashMap<>();
	
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
	public CandidateAssemblyHedge setHedgeCurrencyPairs(List<HedgeCurrencyPair> hedgeCurrencyPairs) {
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
	public CandidateAssemblyHedge setFailedSleepTime(Long failedSleepTime) {
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
	public CandidateAssemblyHedge setCycle(Long cycle) {
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
	public CandidateAssemblyHedge setMarketAvailableDuration(Long marketAvailableDuration) {
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
	public CandidateAssemblyHedge setMaxPlanOrderNumber(Integer maxPlanOrderNumber) {
		this.maxPlanOrderNumber = maxPlanOrderNumber;
		return this;
	}
	
	/**
	 * 对冲组合单的排序规则
	 * <p> 买单在前，卖单在后
	 * <p> 价格高的在前，价格低的在后
	 * 
	 */
	private Comparator<CandidateAssemblyHedgeOrder> hedgeOrderOrderingRule = new Comparator<CandidateAssemblyHedgeOrder>() {

		public int compare(CandidateAssemblyHedgeOrder o1, CandidateAssemblyHedgeOrder o2) {
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
	private BigDecimal feeRate = new BigDecimal("0.002");
	
	/**
	 * 这里认为不同交易所的费率相同
	 * @return 交易手续费费率
	 */
	public BigDecimal getFeeRate(){
		return feeRate;
	}
	
	/**
	 * 设置交易手续费费率
	 * @param feeRate 交易手续费费率， 默认 0.002
	 * @return
	 */
	public CandidateAssemblyHedge setFeeRate(BigDecimal feeRate){
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
	public CandidateAssemblyHedge setPriceScale(Integer priceScale){
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
	public CandidateAssemblyHedge setQuantityScale(Integer quantityScale){
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
	 * 单笔交易基础货币最小量
	 */
	private BigDecimal minQuantity = new BigDecimal("0.01");
	
	/**
	 * 设置单笔交易基础货币最小量
	 * @param minQuantity 交易量
	 */
	public CandidateAssemblyHedge setMinQuantity(BigDecimal minQuantity){
		this.minQuantity = minQuantity;
		return this;
	}
	
	/**
	 * 设置单笔交易基础货币最小量
	 * @return 最小量
	 */
	public BigDecimal getMinQuantity(){
		return this.minQuantity;
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
		logger.debug("开始构建交易所实例 ...");
		// 构建交易所实例
		if (null == exchanges){
			this.exchanges = new HashMap<String, Exchange>();
			
			// 实例化计划对冲单和进行中的对冲单
			this.hedgeCurrencyPairs.stream()
			.forEach(e -> {
				this.planCandidateAssemblyHedgeOrders.put(e, new ArrayList<>());
				this.liveCandidateAssemblyHedgeOrders.put(e, new ArrayList<>());
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
		Map<HedgeCurrencyPair, Depth> result = new ConcurrentHashMap<CandidateAssemblyHedge.HedgeCurrencyPair, Depth>();
		
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
	private Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> CalculateCandidateAssemblyHedgeOrder(Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth){
		Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> assemblyHedgeOrders = new HashMap<>();
		
		// 双币种搬砖套利
		if(hedgeCurrencyPairs.size() == 2){
			return CalculateDoubleCandidateAssemblyHedgeOrder(this.hedgeCurrencyPairs, hedgeCurrencyPairDepth, this.hedgeCurrencyBalances);
		}
		
		
		return assemblyHedgeOrders;
	}
	
	/**
	 * 双币种对对冲计算算法，币种对必须相同。
	 * <p>例如：两个币种对均为 ETH_BTC或者 PNT_BTC 则可以使用此算法。
	 * <br>若两个币种对一种为ETH_BTC，另一种为BTC_ETH，则不能使用此算法。
	 * 
	 * @param mainCurrPair 执行买入操作的币种对
	 * @param viceCurrPair 执行卖出操作的币种对
	 * @param mainCurrPairDepth 执行买入操作币种对的深度
	 * @param viceCurrPairDepth 执行卖出操作币种对的深度
	 * @param mainExchgBal 执行买入操作交易所各币余额
	 * @param viceExchgBal 执行卖出操作的交易所的币的余额
	 * @return 对冲交易订单对
	 */
	private Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> CalculateDoubleCandidateAssemblyHedgeOrder(
			HedgeCurrencyPair mainCurrPair, HedgeCurrencyPair viceCurrPair, 
			Depth mainCurrPairDepth, Depth viceCurrPairDepth, 
			Map<String, Balance> mainExchgBal, Map<String, Balance> viceExchgBal){
		Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> hedgeOrders = new HashMap<>();
		
		List<PriceQuotation> mainCurrPairAsks = mainCurrPairDepth.getAsks();
		List<PriceQuotation> viceCurrPairBids = viceCurrPairDepth.getBids();
		
		String baseCurr = mainCurrPair.getCurrencyPair().split("_")[0]; // 基础币种
		String quoteCurr = mainCurrPair.getCurrencyPair().split("_")[1]; // 计价币种
		
		BigDecimal mainCurrPairAsk1Price = mainCurrPairAsks.get(0).getPrice();	// 主交易所的卖一价
		BigDecimal viceCurrPairBuy1Price = viceCurrPairBids.get(0).getPrice();	// 副交易所的买一价
		// 主交易所的卖一价 > 副交易所的买一价，表示没有对冲机会，因为主交易所总是买入，副交易所总是卖出
		if(mainCurrPairAsk1Price.compareTo(viceCurrPairBuy1Price) > 0){  
			logger.debug("主交易所 {} 卖一价 {}, 副交易所 {} 买一价 {} , 不存在对冲交易机会。", mainCurrPair.getPlatform(), mainCurrPairAsk1Price, viceCurrPair.getPlatform(), viceCurrPairBuy1Price);
			return hedgeOrders;
		}
		
		BigDecimal mainCurrencyPairBuyQuantity = new BigDecimal("0");
		BigDecimal mainCurrencyPairBuyPrice = new BigDecimal("0");
		BigDecimal viceCurrencyPairSellQuantity = new BigDecimal("0");
		BigDecimal viceCurrencyPairSellPrice = new BigDecimal("0");
		while(mainCurrPairAsks.size() > 0 && viceCurrPairBids.size() > 0){
			BigDecimal hedgeBuyQuantity = null;
			BigDecimal hedgeBuyPrice = null;
			BigDecimal hedgeSellQuantity = null;
			BigDecimal hedgeSellPrice = null;
			
			if(mainCurrPairAsks.get(0).getQuantity().compareTo(viceCurrPairBids.get(0).getQuantity()) > 0){
				hedgeSellQuantity = viceCurrPairBids.get(0).getQuantity();
				hedgeSellPrice = viceCurrPairBids.get(0).getPrice();
				hedgeBuyQuantity = hedgeSellQuantity.divide(BigDecimal.ONE.subtract(this.feeRate), quantityScale + 3, RoundingMode.UP);
				hedgeBuyPrice = mainCurrPairAsks.get(0).getPrice();
				
				viceCurrPairBids.remove(0);
				mainCurrPairAsks.get(0).setQuantity(mainCurrPairAsks.get(0).getQuantity().subtract(hedgeBuyQuantity));
			}else{
				hedgeBuyQuantity = mainCurrPairAsks.get(0).getQuantity();
				hedgeBuyPrice = mainCurrPairAsks.get(0).getPrice();
				hedgeSellQuantity = hedgeBuyQuantity.multiply(BigDecimal.ONE.subtract(this.feeRate));
				hedgeSellPrice = viceCurrPairBids.get(0).getPrice();
				mainCurrPairAsks.remove(0);
				viceCurrPairBids.get(0).setQuantity(viceCurrPairBids.get(0).getQuantity().subtract(hedgeSellQuantity));
			}
			
			// 检查是否亏本
			BigDecimal comsumedQuoteCurrency = hedgeBuyQuantity.multiply(hedgeBuyPrice);// 计价货币消耗量
			BigDecimal earnedQuoteCurrency = hedgeSellQuantity.multiply(hedgeSellPrice).multiply(BigDecimal.ONE.subtract(this.feeRate)); // 计价货币增加量
			BigDecimal comsumedBaseCurrency = hedgeSellQuantity; // 基础货币消耗量
			BigDecimal earnedBaseCurrency = hedgeBuyQuantity.multiply(BigDecimal.ONE.subtract(this.feeRate)); // 基础货币增加量
			
			if(logger.isDebugEnabled()){
				
				String mainP = mainCurrPair.getPlatform();
				String viceP = viceCurrPair.getPlatform();
				logger.debug("{} 使用 {} 以 {} 价格买入 {} {}, 消耗 {} × {} = {} {}, 得到 {} - ({} × {}) = {} {} ", mainP, quoteCurr, hedgeBuyPrice
						, hedgeBuyQuantity, baseCurr, hedgeBuyPrice, hedgeBuyQuantity, hedgeBuyPrice.multiply(hedgeBuyQuantity), quoteCurr
						, hedgeBuyQuantity, hedgeBuyQuantity, this.feeRate, earnedBaseCurrency, baseCurr);
				
				logger.debug("{} 使用 {} 以 {} 价格卖出 {} {}, 消耗 {} {}, 得到 {} × {} × (1 - {}) = {} {} ", viceP, quoteCurr, hedgeSellPrice
						, hedgeSellQuantity, baseCurr, hedgeSellQuantity, baseCurr, hedgeSellPrice, hedgeSellQuantity, this.feeRate, earnedQuoteCurrency, quoteCurr);
				
				logger.debug("套利: {} - {} = {}{}, {} - {} = {}{}"
						, earnedBaseCurrency, comsumedBaseCurrency, earnedBaseCurrency.subtract(comsumedBaseCurrency),baseCurr 
						, earnedQuoteCurrency, comsumedQuoteCurrency, earnedQuoteCurrency.subtract(comsumedQuoteCurrency), quoteCurr);
			}
			
			// 计价货币增加了，基础货币没有减少，则表示有套利空间
			if(earnedQuoteCurrency.compareTo(comsumedQuoteCurrency) > 0 && earnedBaseCurrency.compareTo(comsumedBaseCurrency) >= 0){
				mainCurrencyPairBuyQuantity = mainCurrencyPairBuyQuantity.add(hedgeBuyQuantity);
				mainCurrencyPairBuyPrice = hedgeBuyPrice;						
				viceCurrencyPairSellPrice = hedgeSellPrice;
				viceCurrencyPairSellQuantity = viceCurrencyPairSellQuantity.add(hedgeSellQuantity);
			}else{
				break;
			}
		}
		
		// 尽管满足主币种卖一价 < 副币种买一价， 但综合手续费来看不划算。
		if(mainCurrencyPairBuyQuantity.compareTo(BigDecimal.ZERO) <= 0){
			return hedgeOrders;
		}
		
		//可套利
		logger.info("可以套利。");
		BigDecimal mainQuoteCurrBal = mainExchgBal.get(quoteCurr).getFree(); // 计价币种余额
		BigDecimal viceBaseCurrBal = viceExchgBal.get(baseCurr).getFree(); // 基础货币余额
		
		// 保证价格精度正确
		mainCurrencyPairBuyPrice = mainCurrencyPairBuyPrice.divide(BigDecimal.ONE, priceScale, RoundingMode.UP);
		viceCurrencyPairSellPrice = viceCurrencyPairSellPrice.divide(BigDecimal.ONE, priceScale, RoundingMode.DOWN);
		
		// 计价币种余额不足 或 基础货币余额不足
		if(mainQuoteCurrBal.compareTo(mainCurrencyPairBuyPrice.multiply(mainCurrencyPairBuyQuantity)) < 0 || viceBaseCurrBal.compareTo(viceCurrencyPairSellQuantity) < 0){
			// 计价货币比基础货币更充足
			if(mainQuoteCurrBal.divide(mainCurrencyPairBuyPrice, this.quantityScale + 3, RoundingMode.DOWN).compareTo(viceBaseCurrBal) > 0){
				viceCurrencyPairSellQuantity = viceBaseCurrBal.divide(BigDecimal.ONE, this.quantityScale, RoundingMode.DOWN);
				mainCurrencyPairBuyQuantity = viceBaseCurrBal.divide(BigDecimal.ONE.subtract(feeRate), this.quantityScale, RoundingMode.UP);
			}
			// 基础货币比计价货币充足
			else{
				mainCurrencyPairBuyQuantity = mainQuoteCurrBal.divide(mainCurrencyPairBuyPrice, this.quantityScale, RoundingMode.DOWN);
				viceCurrencyPairSellQuantity = mainCurrencyPairBuyQuantity.multiply(BigDecimal.ONE.subtract(feeRate))
						.divide(BigDecimal.ONE, this.quantityScale, RoundingMode.DOWN); 
			}
		}else{
			//两种货币均充足也不要忘了四舍五入
			viceCurrencyPairSellQuantity = viceCurrencyPairSellQuantity.divide(BigDecimal.ONE, this.quantityScale, RoundingMode.DOWN);
			mainCurrencyPairBuyQuantity = mainCurrencyPairBuyQuantity.divide(BigDecimal.ONE, this.quantityScale, RoundingMode.UP);
		}
		
		if(viceCurrencyPairSellQuantity.compareTo(minQuantity) < 0){
			logger.debug("计算出的对冲交易量{}小于设置的最小交易量{}。", viceCurrencyPairSellQuantity.stripTrailingZeros().toPlainString(), minQuantity.toPlainString());
			return hedgeOrders;
		}
		
		logger.debug("最终计算方案：{} 以{}价格买入 {}{}， {} 以 {} 价格卖出 {}{}", 
				mainCurrPair.getPlatform(), mainCurrencyPairBuyPrice, mainCurrencyPairBuyQuantity, baseCurr,
				viceCurrPair.getPlatform(), viceCurrencyPairSellPrice, viceCurrencyPairSellQuantity, baseCurr);
		logger.debug("消耗{} × {} = {}{}", mainCurrencyPairBuyPrice, mainCurrencyPairBuyQuantity, mainCurrencyPairBuyPrice.multiply(mainCurrencyPairBuyQuantity), quoteCurr);
		logger.debug("得到{} × (1 - {}) = {}{}", mainCurrencyPairBuyQuantity, feeRate, mainCurrencyPairBuyQuantity.multiply(BigDecimal.ONE.subtract(feeRate)), baseCurr);
		logger.debug("消耗{}{}", viceCurrencyPairSellQuantity, baseCurr);
		logger.debug("得到{} × {} × (1 - {})={}{}", viceCurrencyPairSellQuantity, viceCurrencyPairSellPrice, 
				feeRate, viceCurrencyPairSellQuantity.multiply(viceCurrencyPairSellPrice).multiply(BigDecimal.ONE.subtract(feeRate)), quoteCurr);
		
		
		
		String hedgeId = UUID.randomUUID().toString().replace("-", "");
		CandidateAssemblyHedgeOrder buyOrder = new CandidateAssemblyHedgeOrder();
		buyOrder.setPlantform(mainCurrPair.getPlatform());
		buyOrder.setOrderQuantity(mainCurrencyPairBuyQuantity);
		buyOrder.setOrderPrice(mainCurrencyPairBuyPrice);
		buyOrder.setHedgeId(hedgeId);
		buyOrder.setCurrencyPair(mainCurrPair.getCurrencyPair());
		buyOrder.setQuoteCurrency(quoteCurr);
		buyOrder.setBaseCurrency(baseCurr);
		buyOrder.setOrderSide("BUY");
		buyOrder.setFeeRate(feeRate);
		
		CandidateAssemblyHedgeOrder sellOrder = new CandidateAssemblyHedgeOrder();
		sellOrder.setPlantform(viceCurrPair.getPlatform());
		sellOrder.setOrderQuantity(viceCurrencyPairSellQuantity);
		sellOrder.setOrderPrice(viceCurrencyPairSellPrice);
		sellOrder.setHedgeId(hedgeId);
		sellOrder.setCurrencyPair(viceCurrPair.getCurrencyPair());
		sellOrder.setQuoteCurrency(quoteCurr);
		sellOrder.setBaseCurrency(baseCurr);
		sellOrder.setOrderSide("SELL");
		sellOrder.setFeeRate(feeRate);
		
		hedgeOrders.put(mainCurrPair, buyOrder);
		hedgeOrders.put(viceCurrPair, sellOrder);
		return hedgeOrders;
	}
	
	
	/**
	 * 双币种对套利，两个币种必须相同
	 * @param hedgeCurrencyPairDepth
	 * @param hedgeCurrencyBalances
	 * @return
	 */
	private Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> CalculateDoubleCandidateAssemblyHedgeOrder(List<HedgeCurrencyPair> hedgeCurrencyPairs,Map<HedgeCurrencyPair, Depth> hedgeCurrencyPairDepth, Map<String, Map<String, Balance>> hedgeCurrencyBalances){
		
		Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> hedgeOrders = new HashMap<>();
		
		if(hedgeCurrencyPairs.size() != 2){
			logger.warn("仅允许操作两个币种对。");
			return hedgeOrders;
		}
		
		HedgeCurrencyPair candidateCurrPairA = hedgeCurrencyPairs.get(0);
		HedgeCurrencyPair candidateCurrPairB = hedgeCurrencyPairs.get(1);
		if(hedgeCurrencyPairDepth.get(candidateCurrPairA).getAsks().get(0).getPrice()
				.compareTo(hedgeCurrencyPairDepth.get(candidateCurrPairB).getBids().get(0).getPrice()) < 0){
			return CalculateDoubleCandidateAssemblyHedgeOrder(candidateCurrPairA, candidateCurrPairB, 
					hedgeCurrencyPairDepth.get(candidateCurrPairA), hedgeCurrencyPairDepth.get(candidateCurrPairB), 
					hedgeCurrencyBalances.get(candidateCurrPairA.getPlatform()), hedgeCurrencyBalances.get(candidateCurrPairB.getPlatform()));
		}else if(hedgeCurrencyPairDepth.get(candidateCurrPairB).getAsks().get(0).getPrice()
				.compareTo(hedgeCurrencyPairDepth.get(candidateCurrPairA).getBids().get(0).getPrice()) < 0){
			return CalculateDoubleCandidateAssemblyHedgeOrder(candidateCurrPairB, candidateCurrPairA, 
					hedgeCurrencyPairDepth.get(candidateCurrPairB), hedgeCurrencyPairDepth.get(candidateCurrPairA), 
					hedgeCurrencyBalances.get(candidateCurrPairB.getPlatform()), hedgeCurrencyBalances.get(candidateCurrPairA.getPlatform()));
						
		}else{
			logger.debug("对冲套利机会不存在。");
		}
		
		return hedgeOrders;
	}
	
	/**
	 * 根据对冲订单组合挂单
	 * @param assemblyHedgeOrders
	 * @return 进行挂单操作之后的组合单，若所有的组合单均挂单失败，则直接清空组合单。
	 */
	private Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> order(final Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> assemblyHedgeOrders){
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
	private CandidateAssemblyHedgeOrder order(final CandidateAssemblyHedgeOrder assemblyHedgeOrder){
		String exchangeName = assemblyHedgeOrder.getPlantform();
		Exchange exchange = exchanges.get(exchangeName);
		String side = assemblyHedgeOrder.getOrderSide();
		String currency = assemblyHedgeOrder.getCurrencyPair();
		BigDecimal quantity = assemblyHedgeOrder.getOrderQuantity();
		BigDecimal price = assemblyHedgeOrder.getOrderPrice();
		
		
		
		assemblyHedgeOrder.setOrderStatus("NEW"); // 此行代码用于模拟下单成功
		assemblyHedgeOrder.setCreateTimestamp(new Long(System.currentTimeMillis()).toString());
		/*
		Order order = exchange.order(side, currency, quantity, price);
		if(null != order){
			assemblyHedgeOrder.setOrderId(order.getOrderId());
			assemblyHedgeOrder.setOrderStatus(OrderStatus.NEW);
		}else{
			assemblyHedgeOrder.setOrderStatus("PLAN");
		}*/
		return assemblyHedgeOrder;
	}
	
	/**
	 * 更新挂单的状态
	 * <p>
	 * 对进行中的订单进行排序，即 <code>liveCandidateAssemblyHedgeOrder</code>，排序规则为
	 * 	<ul>
	 * <li> 卖单优先于买单，即 OrderSide desc
	 * <li> 价格由低到高
	 * <p> 排序之后的最终结果为第一个元素为最高价的买单，最后一个元素为最低价的卖单。
	 * 更新订单时只需要两端开始查询更新即可，因为挂单总是优先成交最高价的买单和最低价的卖单。
	 * 特殊情况是跨两个平台，买价高于卖价，这种情况比较少见，这里认为不可能出现此情况。
	 * 
	 * @param liveCandidateAssemblyHedgeOrders2 进行中的订单 
	 * @return true - 更新成功； false - 更新失败
	 */
	private Boolean updateHedgeOrders(final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> liveCandidateAssemblyHedgeOrders){
		logger.debug("准备更新挂单信息 ...");
		/*for(Entry<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> liveCandidateAssemblyHedgeOrder : liveCandidateAssemblyHedgeOrders.entrySet()){
			HedgeCurrencyPair currencyPair = liveCandidateAssemblyHedgeOrder.getKey();
			List<CandidateAssemblyHedgeOrder> assemblyHedgeOrders = liveCandidateAssemblyHedgeOrder.getValue();
			Collections.sort(assemblyHedgeOrders, this.hedgeOrderOrderingRule);
			Exchange exchange = this.exchanges.get(currencyPair.getPlatform());
			// 更新买单信息
			for(int i=0; i<assemblyHedgeOrders.size(); i++){
				CandidateAssemblyHedgeOrder hedgeOrder = assemblyHedgeOrders.get(i);
				// 第一个订单不是买单，直接跳出
				if(!OrderSide.BUY.equals(hedgeOrder.getOrderSide())){
					break;
				}
				
				Order order = exchange.getOrder(hedgeOrder.getCurrencyPair(), hedgeOrder.getOrderId());
				if(null == order){
					logger.error("获取订单(plantform=,{}, currency={}, orderId={})信息时失败。", hedgeOrder.getPlantform(), hedgeOrder.getCurrencyPair(), hedgeOrder.getOrderId());
					return false;
				}
				hedgeOrder.setOrderStatus(order.getStatus());
				
				String previousStatus = hedgeOrder.getOrderStatus();
				// 订单状态发生了改变
				if(!previousStatus.equals(order.getStatus()) || OrderStatus.FILLED.equals(order.getStatus())){
					hedgeOrder.setOrderStatus(order.getStatus());
					hedgeOrder.setTransPrice(order.getPrice());
					commonDao.saveOrUpdate(hedgeOrder);
					if(OrderStatus.FILLED.equals(hedgeOrder.getOrderStatus())){
						assemblyHedgeOrders.remove(i++);
					}
				}
				// 若价格高的买单状态都发生改变，那价格低的买单肯定也未发生改变，直接跳出循环，不再检查价格低的买单
				else{
					break;
				}
			}
			
			// 更新卖单信息
			for(int i=assemblyHedgeOrders.size() - 1; i>-1; i--){
				
				CandidateAssemblyHedgeOrder hedgeOrder = assemblyHedgeOrders.get(i);
				
				// 最后一个单不是卖单，直接跳出
				if(!OrderSide.SELL.equals(hedgeOrder.getOrderSide())){
					break;
				}
				
				Order order = exchange.getOrder(hedgeOrder.getCurrencyPair(), hedgeOrder.getOrderId());
				if(null == order){
					logger.error("获取订单(plantform=,{}, currency={}, orderId={})信息时失败。", hedgeOrder.getPlantform(), hedgeOrder.getCurrencyPair(), hedgeOrder.getOrderId());
					return false;
				}
				
				String previoursStatus = hedgeOrder.getOrderStatus();
				if(!previoursStatus.equals(order.getStatus()) || OrderStatus.FILLED.equals(order.getStatus())){
					hedgeOrder.setOrderStatus(order.getStatus());
					hedgeOrder.setTransPrice(order.getPrice());
					commonDao.saveOrUpdate(hedgeOrder);
					if(OrderStatus.FILLED.equals(hedgeOrder.getOrderStatus())){
						assemblyHedgeOrders.remove(i--);
					}
				}else{
					break;
				}
			}
		}
		logger.debug("挂单信息更新完成。");
		return true;
	}
	
	private Boolean loadHedgeOrders(final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> liveCandidateAssemblyHedgeOrders){
		logger.debug("正在加载对冲订单信息...");
		
		List<CandidateAssemblyHedgeOrder> openOrders = commonDao.findByHql("from CandidateAssemblyHedgeOrder where orderStatus != 'FILLED'", CandidateAssemblyHedgeOrder.class);
		
		openOrders.stream().forEach(hedgeOrder ->{
			liveCandidateAssemblyHedgeOrders.entrySet().stream()
			.forEach(e -> {
				if(e.getKey().getPlatform().equals(hedgeOrder.getPlantform()) 
						&& e.getKey().getCurrencyPair().equals(hedgeOrder.getCurrencyPair())){
					e.getValue().add(hedgeOrder);
				}
			});
		});
		
		
		logger.debug("对冲订单信息加载完成。");*/
		return true;
	}
	
	/**
	 * 更新各个币种的可用余额
	 * @return
	 */
	private Boolean updateBalances(){/*
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
		logger.debug("账户余额更新完成。");*/
		
		
		for(HedgeCurrencyPair currencyPair : this.hedgeCurrencyPairs){
			String cp = currencyPair.getCurrencyPair();
			String[] cps = cp.split("_");
			Balance p = new Balance();
			p.setFree(new BigDecimal("1000000"));
			Map<String, Balance> bal = new HashMap<>();
			bal.put(cps[0], p);
			bal.put(cps[1], p);
			this.hedgeCurrencyBalances.put(currencyPair.getPlatform(), bal);
			this.hedgeCurrencyBalances.put(currencyPair.getPlatform(), bal);
		}
		
		//this.hedgeCurrencyBalances
		
		return true;
	}
	
	/**
	 * 根据深度信息下计划订单。
	 * <p> 根据深度信息，若买一价大于计划单的卖价，则下卖单；若卖一价小于计划单的买价，则下买单。
	 * <p> 一次做多下一个买单，最多下一个卖单。
	 * @param depths 深度信息
	 * @param planCandidateAssemblyHedgeOrders 经过排序的计划对冲单
	 * @param liveCandidateAssemblyHedgeOrders 进行中的计划对冲单
	 * 
	 */
	private Boolean placePlanHedgeOrders(final Map<HedgeCurrencyPair, Depth> depths,
			final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> planCandidateAssemblyHedgeOrders, 
			final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> liveCandidateAssemblyHedgeOrders){
		logger.debug("准备下计划单 ...");
		boolean isPlacedPlanOrders = false; // 是否下了计划订单标志
		
		for(HedgeCurrencyPair currencyPair : this.hedgeCurrencyPairs){
			Depth depth = depths.get(currencyPair);
			BigDecimal buy1Price = depth.getBids().get(0).getPrice();
			BigDecimal sell1Price = depth.getAsks().get(0).getPrice();
			List<CandidateAssemblyHedgeOrder> planHedgeOrders = planCandidateAssemblyHedgeOrders.get(currencyPair);
			List<CandidateAssemblyHedgeOrder> liveHedgeOrders = liveCandidateAssemblyHedgeOrders.get(currencyPair);
			
			int size = planHedgeOrders.size();
			
			CandidateAssemblyHedgeOrder maxPriceBuyOrder = null;
			CandidateAssemblyHedgeOrder minPriceSellOrder = null;
			
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
	private Boolean placeHedgeOrders(final Map<HedgeCurrencyPair, Depth> currencyPairDepth,
			final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> planHedgeOrders, 
			final Map<HedgeCurrencyPair, List<CandidateAssemblyHedgeOrder>> liveHedgeOrders){
		logger.debug("准备挂对冲单 ...");
		
		Boolean isPlanceOrders = false;
		
		// 计算组合单
		Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> assemblyHedgeOrders = this.CalculateCandidateAssemblyHedgeOrder(currencyPairDepth);
		if(0 == assemblyHedgeOrders.size()){
			logger.info("没有组合对冲交易机会。");
		}else{
			// 挂组合单
			Map<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> hedgeOrders = order(assemblyHedgeOrders);
			for(Entry<HedgeCurrencyPair, CandidateAssemblyHedgeOrder> hedgeOrder : hedgeOrders.entrySet()){
				if("PLAN".equals(hedgeOrder.getValue().getOrderStatus())){
					planHedgeOrders.get(hedgeOrder.getKey()).add(hedgeOrder.getValue());
				}else{
					liveHedgeOrders.get(hedgeOrder.getKey()).add(hedgeOrder.getValue());;
				}
			}
			hedgeOrders.entrySet().parallelStream().forEach(e->commonDao.saveOrUpdate(e.getValue()));
			isPlanceOrders = true;
		}
		logger.debug("对冲单挂单完成。");
		return isPlanceOrders;
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
		
		// 加载进行中的对冲单
		//loadHedgeOrders(this.liveCandidateAssemblyHedgeOrders);
		
		// 更新余额
		updateBalances();
		
		// 开始执行策略
		while(true){
			logger.debug("新一轮。");
			delay(cycle);
			
			// 更新挂单状态
			if(!updateHedgeOrders(this.liveCandidateAssemblyHedgeOrders)){
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
			Boolean isPlacedPlanOrders = this.placePlanHedgeOrders(hedgeCurrencyPairDepth, this.planCandidateAssemblyHedgeOrders, this.liveCandidateAssemblyHedgeOrders); 
			// 已经下了计划订单，考虑到对市场的影响，不再进行后续操作，直接进入下一轮。
			if(isPlacedPlanOrders){
				updateBalances();
				continue;
			}
			
			// 前面操作都 OK 了，开始下组合对冲单
			if(this.placeHedgeOrders(hedgeCurrencyPairDepth, this.planCandidateAssemblyHedgeOrders, this.liveCandidateAssemblyHedgeOrders)){
				updateBalances();
			}
		}
		
	}

	public static void main(String[] args){
		
		List<HedgeCurrencyPair> hedgeCurrencyPairs = new ArrayList<>();
		//hedgeCurrencyPairs.add(new HedgeCurrencyPair("bit-z.com", args[0]));
		//hedgeCurrencyPairs.add(new HedgeCurrencyPair("huobi.pro", args[0]));
		
		
		
		hedgeCurrencyPairs.add(new HedgeCurrencyPair("huobi.pro", "INC_BTC"));
		hedgeCurrencyPairs.add(new HedgeCurrencyPair("bit-z.com", "INC_BTC"));
		new CandidateAssemblyHedge()
		.setCycle(10000L)
		.setFailedSleepTime(30000L)
		.setHedgeCurrencyPairs(hedgeCurrencyPairs)
		.setMarketAvailableDuration(5000L)
		.setMaxPlanOrderNumber(10)
		.setFeeRate(new BigDecimal("0.002"))
		.setQuantityScale(2)
		.setPriceScale(8)
		.setMinQuantity(new BigDecimal("0.01"))
		.run(null);
		
		
		
		
		
		//System.out.println(EndExchangeFactory.newInstance("huobi.pro").getOrder("INC_ETH", "5337727881"));
		
		
		//EndExchangeFactory.newInstance("bit-z.com").getHistoryOrders("INC_ETH");
		//EndExchangeFactory.newInstance("bit-z.com").order("BUY", "INC_ETH", new BigDecimal("30.00000000"), new BigDecimal("0.00043499"));
		//List<Order> orders = EndExchangeFactory.newInstance("bit-z.com").getOpenOrders("PNT_ETH");
	}
	
}
