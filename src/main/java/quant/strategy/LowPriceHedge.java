package quant.strategy;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import exunion.exchange.Exchange;
import exunion.metaobjects.Account;
import exunion.metaobjects.Depth;
import exunion.metaobjects.Order;
import exunion.metaobjects.OrderStatus;
import quant.dao.CommonDao;
import quant.entity.LowPriceHedgeOrder;
import quant.utils.TimeUtil;


/**
 * 低价对冲策略。
 * 
 * 利用交易所价格精度固定的缺陷，当某一种币的价格极低时，在BTC市场能达到最小精度。
 * 这时，只要在买一价买入，在卖一价卖出就能套利。
 * <p>例如: BCN/BTC 的买一价是 0.00000078 卖一价是 0.00000079则只要在买一价买到
 * 在卖一价卖出，即可套利。而买到和卖掉的概率相对较高。
 * 
 * @author robothy
 *
 */
public class LowPriceHedge implements Strategy {

	/***********************  常量定义 *************************/
	private static final Logger logger = LogManager.getLogger("quant");
	
	private static final CommonDao commonDao = new CommonDao();
	
	
	  
	
	// 排序规则
	private final Comparator<LowPriceHedgeOrder> orderRule = (LowPriceHedgeOrder o1, LowPriceHedgeOrder o2) 
			-> {
				int side = o1.getOrderSide().compareTo(o2.getOrderSide());
				int price = o2.getOrderPrice().compareTo(o1.getOrderPrice());
				return side != 0 ? side : price; 
			};
	
	/*********************************************************/
	
	
	
	/***********************  参数定义 *************************/
	
	// 交易所名称
	private String exchangeName = null;
	
	// 获取交易所名称
	public String getExchangeName(){
		return this.exchangeName;
	}
	
	// 设置交易所名称
	public LowPriceHedge setExchangeName(String exchangeName){
		this.exchangeName = exchangeName;
		return this;
	}
	
	// 币种对
	private String currencyPair = null;
	
	// 获取币种对
	public String getCurrencyPair(){
		return this.currencyPair;
	}
	
	// 设置币种对
	public LowPriceHedge setCurrencyPair(String currencyPair){
		this.currencyPair = currencyPair;
		return this;
	}
	
	// 最高价
	private BigDecimal highestPrice = null;
	
	// 设置对冲操作的最高价
	public LowPriceHedge setHighestPrice(BigDecimal highestPrice){
		this.highestPrice = highestPrice;
		return this;
	}
	
	// 获取对冲操作的最高价
	public BigDecimal getHighestPrice(){
		return this.highestPrice;
	}
	
	//API操作失败休息时间， （单位：毫秒），默认 1000ms
	private Long failedSleepTime = 30000L;
	
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
	public LowPriceHedge setFailedSleepTime(Long failedSleepTime) {
		this.failedSleepTime = failedSleepTime;
		return this;
	}
	
	//操作周期, 默认10毫秒
	private Long cycle = 5000L;
	
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
	public LowPriceHedge setCycle(Long cycle) {
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
	public LowPriceHedge setFeeRate(BigDecimal feeRate){
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
	public LowPriceHedge setPriceScale(Integer priceScale){
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
	public LowPriceHedge setQuantityScale(Integer quantityScale){
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
	private BigDecimal minQuantity = null;
	
	/**
	 * 设置单笔交易基础货币最小量
	 * @param minQuantity 交易量
	 */
	public LowPriceHedge setMinQuantity(BigDecimal minQuantity){
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
	
	/**********************************************************/
	
	/*========================全局参数定义=====================*/
	
	// 计价货币
	private String quoteCurrency = null;
	
	// 获取计价货币
	public String getQuoteCurrency(){
		return this.quoteCurrency;
	}
	
	// 基础货币
	private String baseCurrency = null;
	
	// 获取基础货币
	public String getBaseCurrency(){
		return this.baseCurrency;
	}
	
	// 对冲深度
	private Integer hedgeLenth = null;
	
	/**
	 * 设置对冲深度，即挂出的买单和卖单的最大数量，一般以预估的瞬间波动为准。
	 * @param hedgeLenth
	 */
	public LowPriceHedge setHedgeLenth(Integer hedgeLenth){
		this.hedgeLenth = hedgeLenth;
		return this;
	}
	
	// 获取对冲深度
	public Integer getHedgeLenth(){
		return this.hedgeLenth;
	}
	
	/**
	 * 未成交的订单
	 */
	private List<LowPriceHedgeOrder> unfilledOrders = null;
	
	/**
	 * 交易所实例
	 */
	private Exchange exchange = null;
	
	/**
	 * 获取交易所实例
	 */
	public Exchange getExchange(){
		return this.exchange;
	}
	
	// 计价货币余额
	private BigDecimal quoteCurrencyBalance = null;
	
	// 获取计价货币余额
	public BigDecimal getQuoteCurrencyBalance(){
		return this.quoteCurrencyBalance;
	}
	
	// 基础货币余额
	private BigDecimal baseCurrencyBalance = null;
	
	// 获取基础货币余额
	public BigDecimal getBaseCurrencyBalance(){
		return this.baseCurrencyBalance;
	}
	
	/*--------------------------------------------------------*/
	
	
	/*=======================私有方法定义=======================*/
	
	/**
	 * 校验参数
	 * @return
	 */
	private Boolean validateParameters(){
		Boolean result = true;
		logger.info("参数校验开始。");
		if(null == currencyPair){
			logger.error("未设置交易币种对。");
			result = false;
		}
		
		if(null == exchangeName){
			logger.error("未指定交易所。");
			result = false;
		}
		
		if(null == highestPrice){
			logger.warn("未设置最高操作价格，最高操作价格将设置为 0.00000200");
			this.highestPrice = new BigDecimal("0.000002");
		}
		
		if(null == this.priceScale){
			logger.error("未设置价格精度。");
			result = false;
		}
		
		if(null == this.quantityScale){
			logger.error("未设置量精度");
			result = false;
		}
		
		if(null == this.minQuantity){
			logger.error("未设置单次挂单最小量。");
			result = false;
		}
		
		if(null == this.hedgeLenth){
			logger.warn("未设置对冲的最大深度，最大深度将被设置为默认值 10");
			this.hedgeLenth = 10;
		}
		
		logger.info("参数校验完成。");
		return result;
	}
	
	/**
	 * 初始化
	 */
	private Boolean init(){
		Boolean result = false;
		logger.info("正在初始化 LowPriceHedge 策略。");
		String[] currArry = this.currencyPair.split("_");
		this.baseCurrency = currArry[0];
		this.quoteCurrency = currArry[1];
		logger.info("策略初始化完成。");
		return result;
	}
	
	/**
	 * 启动策略时从数据库加载进行中的订单信息。
	 * @return
	 */
	private Boolean loadUnfilledOrders(){
		Boolean result = true;
		logger.info("正在从数据库中加载进行中的 LOW_PRICE_HEDGE_ORDER_PAIR 订单...");
		this.unfilledOrders = commonDao.findByHql("from LowPriceHedgeOrder where orderStatus != 'FILLED'", LowPriceHedgeOrder.class);
		Collections.sort(this.unfilledOrders, this.orderRule);
		logger.info("从数据库中加载 LOW_PRICE_HEDGE_ORDER_PAIR 订单完成。");
		return result;
	}
	
	/**
	 * 更新余额
	 */
	private Boolean updateBalance(){
		logger.info("正在更新余额 ...");
		Account account =  this.exchange.getAccount();
		if(null == account){
			logger.error("获取账户信息失败，无法更新余额。");
			return false;
		}
		this.quoteCurrencyBalance = account.getBalance(this.quoteCurrency).getFree();
		this.baseCurrencyBalance = account.getBalance(this.baseCurrency).getFree();
		logger.info("余额更新完成。");
		return true;
	}
	
	/**
	 * 在盘口下单
	 * @param depth 深度信息
	 * @return
	 */
	private Boolean placeOrder(Depth depth){
		
		BigDecimal buy1Price = depth.getBids().get(0).getPrice();
		BigDecimal sell1Price = depth.getAsks().get(0).getPrice();
		
		LowPriceHedgeOrder firstUnfilledOrder = null;
		
		LowPriceHedgeOrder lastUnfilledOrder = null;
		
		if(sell1Price.compareTo(this.highestPrice) >= 0){
			logger.error("当前{}价格已超过对冲的最高价{}", currencyPair, highestPrice);
			return false;
		}
		
		if(this.unfilledOrders.size() > 0){
			firstUnfilledOrder = this.unfilledOrders.get(0);
			lastUnfilledOrder = this.unfilledOrders.get(this.unfilledOrders.size() - 1);
		}
		
		if((sell1Price.compareTo(firstUnfilledOrder.getOrderPrice()) != 0
				&& buy1Price.compareTo(lastUnfilledOrder.getOrderPrice()) !=0) || this.unfilledOrders.size() == 0){
			
			String hedgeId = UUID.randomUUID().toString().replace("-", "");
			
			LowPriceHedgeOrder sellOrder = new LowPriceHedgeOrder();
			sellOrder.setCurrencyPair(currencyPair);
			sellOrder.setCreateTimestamp(System.currentTimeMillis());
			sellOrder.setHedgeId(hedgeId);
			sellOrder.setExchangeName(exchangeName);
			sellOrder.setFeeRate(feeRate);
			sellOrder.setOrderPrice(sell1Price);
			BigDecimal sellQuantity = caclQuantity(depth);
			sellOrder.setOrderQuantity(sellQuantity);
			sellOrder.setOrderSide("SELL");
			
			LowPriceHedgeOrder buyOrder = new LowPriceHedgeOrder();
			buyOrder.setCurrencyPair(currencyPair);
			buyOrder.setCreateTimestamp(System.currentTimeMillis());
			buyOrder.setHedgeId(hedgeId);
			buyOrder.setExchangeName(exchangeName);
			buyOrder.setFeeRate(feeRate);
			buyOrder.setOrderPrice(buy1Price);
			BigDecimal buyQuantity = sellQuantity.multiply(this.feeRate)
					.divide(BigDecimal.ONE, this.quantityScale, RoundingMode.UP);
			buyOrder.setOrderQuantity(buyQuantity);
			buyOrder.setOrderSide("BUY");
			if(buyQuantity.compareTo(this.minQuantity) < 0){
				logger.error("计算出的量{}小于最小可买量{}", buyQuantity, minQuantity);
				return true;
			}
			
			LowPriceHedgeOrder sellOrderResult = this.order(sellOrder);
			LowPriceHedgeOrder buyOrderResult = this.order(buyOrder);
			if(sellOrderResult == null && buyOrderResult !=null){
				sellOrder.setOrderStatus("PLAN");
			}else if(sellOrderResult != null && buyOrderResult == null){
				buyOrder.setOrderStatus("PLAN");
			}else if(sellOrderResult == null && buyOrderResult == null){
				return false;
			}
			commonDao.saveOrUpdate(sellOrder);
			commonDao.saveOrUpdate(buyOrder);
			return true;
		}
		
		return true;
	}
	
	private BigDecimal caclQuantity(Depth depth){
		BigDecimal quantityByBaseCurr = baseCurrencyBalance.divide(new BigDecimal("2").multiply(new BigDecimal(this.hedgeLenth)), 
				this.quantityScale, RoundingMode.DOWN);
		BigDecimal quantityByQuoteCurr = quoteCurrencyBalance
				.divide(new BigDecimal("2").multiply(new BigDecimal(this.hedgeLenth)), this.quantityScale + 12)
				.divide(depth.getBids().get(0).getPrice(), this.quantityScale, RoundingMode.DOWN);
		return quantityByBaseCurr.compareTo(quantityByQuoteCurr) > 0 ? quantityByQuoteCurr : quantityByBaseCurr;
	}
	
	/**
	 * 调整订单
	 * @param depth
	 * @return
	 */
	private Boolean adjustOrder(Depth depth){
		logger.debug("准备下计划单 ...");
		
		long newBuyOrderNum = this.unfilledOrders.stream()
				.filter(e -> "NEW".equals(e.getOrderStatus()))
				.filter(e -> "BUY".equals(e.getOrderSide()))
				.count();
		
		long planBuyOrderNum = this.unfilledOrders.stream()
				.filter(e -> "PLAN".equals(e.getOrderStatus()))
				.filter(e -> "BUY".equals(e.getOrderSide()))
				.count();
		
		// 新订单的数量超过了对冲单的长度
		if(newBuyOrderNum > this.hedgeLenth){
			logger.debug("新买单的数量{}大于对冲单的最大长度{}", newBuyOrderNum, hedgeLenth);
			if(null == newOrderToPlanOrder(this.unfilledOrders.get((int) (newBuyOrderNum - 1)))){
				return false;
			}else{
				return true;
			}
		}
		
		// 新订单的数量少于对冲长度，且有计划买单，将计划买单转化为买单
		if(newBuyOrderNum < this.hedgeLenth && planBuyOrderNum > 0){
			logger.debug("新买单的数量{}小于对冲单的最大长度{}且存在{}个计划买单。", newBuyOrderNum, hedgeLenth, planBuyOrderNum);
			if(null == planOrderToNewOrder(this.unfilledOrders.get((int) newBuyOrderNum))){
				return true;
			}else{
				return false;
			}
		}
		
		long newSellOrderNum = this.unfilledOrders.stream()
				.filter(e -> "SELL".equals(e.getOrderSide()))
				.filter(e -> "NEW".equals(e.getOrderStatus()))
				.count();
		
		long planSellOrderNum = this.unfilledOrders.stream()
				.filter(e -> "SELL".equals(e.getOrderSide()))
				.filter(e -> "PLAN".equals(e.getOrderStatus()))
				.count();
		
		if(newSellOrderNum > this.hedgeLenth){
			logger.debug("新卖单的数量{}超过了最大对冲长度{}", newSellOrderNum, hedgeLenth);
			if(null == newOrderToPlanOrder(this.unfilledOrders.get((int) (this.unfilledOrders.size() - newSellOrderNum -1)))){
				return false;
			}else{
				return true;
			}
		}
		
		if(newSellOrderNum < this.hedgeLenth && planSellOrderNum > 0){
			logger.debug("新卖单的数量{}小于最大对冲长度{}且存在{}计划卖单。", newSellOrderNum, hedgeLenth, planSellOrderNum);
			if(null == planOrderToNewOrder(this.unfilledOrders.get((int) (this.unfilledOrders.size() - newSellOrderNum -2)))){
				return false;
			}else{
				return true;
			}
		}
		
		logger.debug("计划单下单完成。");
		return true;
	}
	
	/**
	 * 挂单
	 * @param hedgeOrder 对冲单
	 * @return
	 */
	private LowPriceHedgeOrder order(final LowPriceHedgeOrder hedgeOrder){
		Order order = exchange.order(hedgeOrder.getOrderSide(), hedgeOrder.getCurrencyPair(), hedgeOrder.getOrderQuantity(), hedgeOrder.getOrderPrice());
		if(null != order){
			hedgeOrder.setOrderStatus(OrderStatus.NEW);
			hedgeOrder.setOrderId(order.getOrderId());
			hedgeOrder.setModifyTimestamp(System.currentTimeMillis());
			return hedgeOrder;
		}
		return null;
	}
		
	/**
	 * 将进行中的订单转化为计划单，一般在针对不太可能很快成交的订单，暂时将这些订单取消，以挪出资金来挂新的单
	 * @param newOrder
	 * @return
	 */
	private LowPriceHedgeOrder newOrderToPlanOrder(final LowPriceHedgeOrder newOrder){
		Order order = this.exchange.cancel(newOrder.getCurrencyPair(), newOrder.getOrderId());
		if(null != order){
			newOrder.setOrderId(null);
			newOrder.setOrderStatus("PLAN");
			return newOrder;
		}
		return null;
	}
	
	/**
	 * 将计划单转化为进行中订单。
	 * @param planOrder
	 * @return
	 */
	private LowPriceHedgeOrder planOrderToNewOrder(final LowPriceHedgeOrder planOrder){
		return this.order(planOrder);
	}
	
	/*--------------------------------------------------------*/
	
	
	
	
	@Override
	public void run(Map<String, Object> parameters) {
		
		// 校验参数
		this.validateParameters();
		
		// 初始化策略
		this.init();
		
		// 加载未完成的订单
		this.loadUnfilledOrders();
		
		// 个更新余额
		this.updateBalance();
		
		while(true){
			TimeUtil.delay(this.cycle);
			logger.debug("新一轮。");
			Depth depth = exchange.getDepth(currencyPair);
			if(depth == null){
				logger.error("获取深度信息失败，{}秒后进行下一轮操作。", this.failedSleepTime / 1000);
				continue;
			}
			
			if(this.placeOrder(depth)){
				logger.error("下单失败，{}秒后进行下一轮操作。", this.failedSleepTime / 1000);
				continue;
			}
			
			if(!adjustOrder(depth)){
				logger.error("调整订单失败，{}秒后进行下一轮。", this.failedSleepTime / 1000);
				TimeUtil.delay(this.failedSleepTime);
			}
			
		}
	}
	
	public static void main(String[] args){
		List<Integer> l = new ArrayList<>();
		Integer i = new Integer(1);
		l.add(i);
		l.add(i);
		l.add(i);
		System.out.println(l);
	}

}
