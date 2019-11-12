package quant.strategy;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
import exunion.metaobjects.OrderSide;
import exunion.metaobjects.OrderStatus;
import quant.dao.CommonDao;
import quant.entity.WaveHedgeOrder;
import quant.exchange.EndExchangeFactory;
import quant.utils.TimeUtil;


/**
 * 波段操作策略，将账户余额分成多份，同时挂买单和卖单，当买单和卖单均成交时套利成功。
 * @author robothy
 *
 */
public class WaveHedge implements Strategy {

	/***********************  常量定义 *************************/
	private static final Logger logger = LogManager.getLogger("quant");
	
	private static final CommonDao commonDao = new CommonDao();
	
	
	  
	
	// 排序规则
	private final Comparator<WaveHedgeOrder> orderRule = (WaveHedgeOrder o1, WaveHedgeOrder o2) 
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
	public WaveHedge setExchangeName(String exchangeName){
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
	public WaveHedge setCurrencyPair(String currencyPair){
		this.currencyPair = currencyPair;
		return this;
	}
	
	// 最高价
	private BigDecimal highestPrice = null;
	
	// 设置对冲操作的最高价
	public WaveHedge setHighestPrice(BigDecimal highestPrice){
		this.highestPrice = highestPrice;
		return this;
	}
	
	// 获取对冲操作的最高价
	public BigDecimal getHighestPrice(){
		return this.highestPrice;
	}
	
	// 操作的最低价
	private BigDecimal lowestPrice = null;
	
	/**
	 * 设置操作的最低价
	 * @param lowestPrice 最低价
	 */
	public WaveHedge setLowestPrice(BigDecimal lowestPrice){
		this.lowestPrice = lowestPrice;
		return this;
	}

	/**
	 * 获取操作的最低价
	 * @return 最低价
	 */
	public BigDecimal getLowestPrice(){
		return this.lowestPrice;
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
	public WaveHedge setFailedSleepTime(Long failedSleepTime) {
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
	public WaveHedge setCycle(Long cycle) {
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
	 * 交易手续费费率
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
	public WaveHedge setFeeRate(BigDecimal feeRate){
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
	public WaveHedge setPriceScale(Integer priceScale){
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
	public WaveHedge setQuantityScale(Integer quantityScale){
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
	public WaveHedge setMinQuantity(BigDecimal minQuantity){
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
	
	/**
	 * 交易对之间的间隔比率。
	 * <p>例如:间隔比率是 0.1，则相邻两单的卖价可以为 1 和 1.1, 因为 (1.1 - 1) ÷ 1 = 0.1
	 */
	private BigDecimal intervalRate = null;
	
	/**
	 * 设置交易对之间的间隔比率。
	 * <p>例如:间隔比率是 0.1，则相邻两单的卖价可以为 1 和 1.1, 因为 (1.1 - 1) ÷ 1 = 0.1
	 */
	public WaveHedge setIntervalRate(BigDecimal intervalRate){
		this.intervalRate = intervalRate;
		return this;
	}
	
	/**
	 * 获取交易对之间的间隔比率，例如：
	 * <p>间隔比率是 0.1，则相邻两单的卖价可以为 1 和 1.1, 因为 (1.1 - 1) ÷ 1 = 0.1
	 * @return 交易对之间的间隔比率
	 */
	public BigDecimal getIntervalRate(){
		return this.intervalRate;
	}
	
	/**
	 * 刨除手续费之后的最小套利空间
	 */
	private BigDecimal minMarginRate = null;
	
	/**
	 * 获取刨除手续费之后的最小套利空间
	 * @return 最小套利空间
	 */
	public BigDecimal getMinMarginRate(){
		return this.minMarginRate;
	}
	
	/**
	 * 设置刨除手续费之后的最小套利空间
	 * @param minMarginRate 最小套利空间
	 */
	public WaveHedge setMinMarginRate(BigDecimal minMarginRate){
		this.minMarginRate = minMarginRate;
		return this;
	}
	
	/**
	 * 基础币获利比率与计价币获利比率的比值。
	 * <p> 例如 0 则表示基础币比获利, 1表示基础币与计价币各获利一般。
	 */
	private BigDecimal marginSpecificValue = null;
	
	/**
	 * 设置基础币获利比率与计价币获利比率的比值。
	 * <p> 例如 0 则表示基础币比获利, 1表示基础币与计价币各获利一般。
	 */
	public WaveHedge setMarginSpecificValue(BigDecimal marginSpecificValue){
		this.marginSpecificValue = marginSpecificValue;
		return this;
	}
	
	/**
	 * 获取基础币获利比率与计价币获利比率的比值。
	 * <p> 例如 0 则表示基础币比获利, 1表示基础币与计价币各获利一般。
	 */
	public BigDecimal getMarginSpecificValue(){
		return this.marginSpecificValue;
	}
	
	/**
	 * 操作数量
	 */
	private BigDecimal quantity;
	
	/**
	 * 设置操作数量
	 * @param quantity
	 * @return
	 */
	public WaveHedge setQuantity(BigDecimal quantity){
		this.quantity = quantity;
		return this;
	}
	
	/**
	 * 获取操作数量
	 * @return
	 */
	public BigDecimal getQuantity(){
		return this.quantity;
	}
	
	/**
	 * 更新余额频率
	 */
	private Integer updateBalanceFrequency = 1;
	
	/**
	 * 设置更新余额的频率。
	 * <p> 余额在本策略特定的条件下会更新，但有些本策略之外的操作也会影响余额。
	 * 例如转入币，本策略并不能检测到这类操作。所以引入频率机制来更新余额，即策略每
	 * 循环 updateBalanceFrequency 轮就更新一次余额。
	 * @param updateBalanceFrequency
	 */
	public WaveHedge setUpdateBalanceFrequency(Integer updateBalanceFrequency){
		this.updateBalanceFrequency = updateBalanceFrequency;
		return this;
	}

	/**
	 * 获取更新余额的频率。
	 * <p> 余额在本策略特定的条件下会更新，但有些本策略之外的操作也会影响余额。
	 * 例如转入币，本策略并不能检测到这类操作。所以引入频率机制来更新余额，即策略每
	 * 循环 updateBalanceFrequency 轮就更新一次余额。
	 * @return
	 */
	public Integer getUpdateBalanceFrequency(){
		return this.updateBalanceFrequency;
	}
	
	/**
	 * 更新挂单频率
	 */
	private Integer updateOrderFrequency = 1;
	
	/**
	 * 设置更新订单的频率
	 * @param updateOrderFrequency
	 * @return
	 */
	public WaveHedge setUpdateOrderFrequency(Integer updateOrderFrequency){
		this.updateOrderFrequency = updateOrderFrequency;
		return this;
	}
	
	/**
	 * 获取更新订单的频率
	 * @return
	 */
	public Integer getUpdateOrderFrequency(){
		return this.updateOrderFrequency;
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
	public WaveHedge setHedgeLenth(Integer hedgeLenth){
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
	private List<WaveHedgeOrder> unfilledOrders = null;
	
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
			logger.error("未设置最高操作价格。");
			result = false;
		}
		
		if(null == lowestPrice){
			logger.error("未设置最低操作价格。");
			result = false;
		}
		
		if(null != lowestPrice && null != highestPrice && highestPrice.compareTo(lowestPrice) < 0){
			logger.error("最低操作价格必须低于最高操作价格。");
			result = false;
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
		
		if(null == this.intervalRate){
			logger.error("未设置交易对之间的间隔比率。");
			result = false;
		}
		
		if(null == minMarginRate){
			logger.error("未设置扣除手续费后最小利润率。");
			result = false;
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
		this.exchange = EndExchangeFactory.newInstance("hadax.com");
		
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
		this.unfilledOrders = commonDao.findByHql("from WaveHedgeOrder where orderStatus != 'FILLED' and exchangeName='" + exchangeName + "' and currencyPair='" + currencyPair + "'", WaveHedgeOrder.class);
		Collections.sort(this.unfilledOrders, this.orderRule);
		logger.info("从数据库中加载 WAVE_HEDGE_ORER 订单完成。");
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
	 * 更新挂单信息
	 * @return
	 */
	private Boolean updateOrders(){
		
		Boolean isOrderChanged = false;
		
		for(int i=0; i<this.unfilledOrders.size(); i++){
			WaveHedgeOrder buyOrder = this.unfilledOrders.get(i);
			if(!OrderSide.BUY.equals(buyOrder.getOrderSide()) || "PLAN".equals(buyOrder.getOrderStatus())){
				break;
			}
			String previrousStatus = buyOrder.getOrderStatus();
			Order order = exchange.getOrder(buyOrder.getCurrencyPair(), buyOrder.getOrderId());
			if(null == order){
				return false;
			}
			if(previrousStatus.equals(order.getStatus())){
				break;
			}
			
			buyOrder.setOrderStatus(order.getStatus());
			buyOrder.setModifyTimestamp(System.currentTimeMillis());
			commonDao.saveOrUpdate(buyOrder);
			this.unfilledOrders.remove(i--);
			isOrderChanged = true;
			TimeUtil.delay(cycle);
		}
		
		for(int i=this.unfilledOrders.size() -1; i>0; i--){
			WaveHedgeOrder sellOrder = this.unfilledOrders.get(i);
			if(!OrderSide.SELL.equals(sellOrder.getOrderSide()) || "PLAN".equals(sellOrder.getOrderStatus())){
				break;
			}
			String previrousStatus = sellOrder.getOrderStatus();
			Order order = exchange.getOrder(sellOrder.getCurrencyPair(), sellOrder.getOrderId());
			if(null == order){
				return false;
			}
			if(previrousStatus.equals(order.getStatus())){
				break;
			}
			
			sellOrder.setOrderStatus(order.getStatus());
			sellOrder.setModifyTimestamp(System.currentTimeMillis());
			commonDao.saveOrUpdate(sellOrder);
			this.unfilledOrders.remove(i);
			isOrderChanged = true;
			TimeUtil.delay(cycle);
		}
		
		if(isOrderChanged){
			updateBalance();
		}
		logger.debug("订单信息更新完成。");
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
		
		if(sell1Price.compareTo(this.highestPrice) > 0){
			logger.error("当前卖一价{}已超过波段操作的最高价{}", currencyPair, highestPrice);
			return false;
		}
		
		if(buy1Price.compareTo(this.lowestPrice) < 0){
			logger.error("当前买一价{}已低于波段操作的最低价格{}", currencyPair, lowestPrice);
			return false;
		}
		
		BigDecimal maxBuyPrice = null;
		
		BigDecimal minSellPrice = null;
		
		if(this.unfilledOrders.size() != 0){
			// 存在买单
			if(OrderSide.BUY.equals(this.unfilledOrders.get(0).getOrderSide())){
				maxBuyPrice = this.unfilledOrders.get(0).getOrderPrice();
			}else{
				maxBuyPrice = lowestPrice;
			}
			
			// 存在卖单
			if(OrderSide.SELL.equals(this.unfilledOrders.get(unfilledOrders.size() - 1).getOrderSide())){
				minSellPrice = this.unfilledOrders.get(unfilledOrders.size() - 1).getOrderPrice();
			}else{
				minSellPrice = highestPrice;
			}
		}else{
			maxBuyPrice = lowestPrice;
			minSellPrice = highestPrice;
		}
		
		BigDecimal orderLowerBound = maxBuyPrice.multiply(this.intervalRate.add(BigDecimal.ONE));
		BigDecimal orderUpperBound = minSellPrice.divide(this.intervalRate.add(BigDecimal.ONE), this.priceScale + 3, RoundingMode.HALF_EVEN);
		
		
		BigDecimal minPriceRate = minMarginRate.add(feeRate);
		//最高买价和最低卖价之间仍然能够组成套利组合
		if(orderUpperBound.subtract(orderLowerBound).divide(orderLowerBound, this.priceScale + 3, RoundingMode.DOWN).compareTo(minPriceRate) >= 0){
			BigDecimal sumPrice = buy1Price.add(sell1Price);
			BigDecimal buyPrice = sumPrice.divide(BigDecimal.ONE.add(BigDecimal.ONE).add(minPriceRate), priceScale + 3, RoundingMode.HALF_UP);
			BigDecimal sellPrice = BigDecimal.ONE.add(minPriceRate).multiply(sumPrice).divide(BigDecimal.ONE.add(BigDecimal.ONE).add(minPriceRate), priceScale + 3, RoundingMode.HALF_UP);
			if(buyPrice.compareTo(orderLowerBound) < 0){
				buyPrice = orderLowerBound;
				sellPrice = orderLowerBound.multiply(BigDecimal.ONE.add(minPriceRate));
			}else if(sellPrice.compareTo(orderUpperBound) > 0){
				sellPrice = orderUpperBound;
				buyPrice = orderUpperBound.divide(BigDecimal.ONE.add(minPriceRate), priceScale + 3, RoundingMode.DOWN);
			}
			
			// 计算下单数量
			BigDecimal sellQuantity = this.quantity;
			BigDecimal buyQuantity = sellQuantity.divide(BigDecimal.ONE.subtract(feeRate), quantityScale + 3, RoundingMode.UP);
			
			// 调整精度
			buyQuantity = buyQuantity.divide(BigDecimal.ONE, quantityScale, RoundingMode.UP);
			sellQuantity = sellQuantity.divide(BigDecimal.ONE, quantityScale, RoundingMode.DOWN);
			buyPrice = buyPrice.divide(BigDecimal.ONE, priceScale, RoundingMode.DOWN);
			sellPrice = sellPrice.divide(BigDecimal.ONE, priceScale, RoundingMode.UP);
			
			// 判断余额是否充足
			if(this.baseCurrencyBalance.compareTo(sellQuantity) <=0 
				|| this.quoteCurrencyBalance.compareTo(buyQuantity.multiply(buyPrice)) <= 0){
				logger.error("余额不足，计价币{}剩余{},基础币{}剩余{}", quoteCurrency, quoteCurrencyBalance, baseCurrency, baseCurrencyBalance);
				return false;
			}
			
			
			String hedgeId = UUID.randomUUID().toString().replace("-", "");
			
			WaveHedgeOrder buyOrder = buildWaveHedgeOrder(OrderSide.BUY, buyPrice, buyQuantity, hedgeId);
			WaveHedgeOrder sellOrder = buildWaveHedgeOrder(OrderSide.SELL, sellPrice, sellQuantity, hedgeId);
			buyOrder = this.order(buyOrder);
			sellOrder = this.order(sellOrder);
			if("PLAN".equals(buyOrder.getOrderStatus()) && "PLAN".equals(sellOrder.getOrderStatus())){
				return false;
			}else{
				commonDao.saveOrUpdate(buyOrder);
				commonDao.saveOrUpdate(sellOrder);
				this.unfilledOrders.add(0, buyOrder);
				this.unfilledOrders.add(this.unfilledOrders.size(), sellOrder);
				updateBalance();
			}
		}
		return true;
	}
	
	/**
	 * 下计划单
	 * @return
	 */
	private Boolean placePlanOrder(){
		Boolean result = true;
		this.unfilledOrders
		.parallelStream()
		.filter(e ->  "PLAN".equals(e.getOrderStatus()))
		.forEach(e -> {
			WaveHedgeOrder order = this.order(e);
			if(!"PLAN".equals(order.getOrderStatus())){
				commonDao.saveOrUpdate(order);
			}
		});
		return result;
		
	}
	
	private WaveHedgeOrder buildWaveHedgeOrder(String orderSide, BigDecimal orderPrice, BigDecimal orderQuantity, String hedgeId){
		WaveHedgeOrder waveHedgeOrder = new WaveHedgeOrder();
		waveHedgeOrder.setCreateTimestamp(System.currentTimeMillis());
		waveHedgeOrder.setCurrencyPair(this.currencyPair);
		waveHedgeOrder.setExchangeName(this.exchangeName);
		waveHedgeOrder.setFeeRate(this.feeRate);
		waveHedgeOrder.setHedgeId(hedgeId);
		waveHedgeOrder.setOrderPrice(orderPrice);
		waveHedgeOrder.setOrderQuantity(orderQuantity);
		waveHedgeOrder.setOrderSide(orderSide);
		return waveHedgeOrder;
	}
	
	/**
	 * 挂单
	 * @param hedgeOrder 对冲单
	 * @return
	 */
	private WaveHedgeOrder order(final WaveHedgeOrder hedgeOrder){
		Order order = exchange.order(hedgeOrder.getOrderSide(), hedgeOrder.getCurrencyPair(), hedgeOrder.getOrderQuantity(), hedgeOrder.getOrderPrice());
		if(null != order){
			hedgeOrder.setOrderStatus(OrderStatus.NEW);
			hedgeOrder.setOrderId(order.getOrderId());
		}else{
			hedgeOrder.setOrderStatus("PLAN");
		}
		hedgeOrder.setModifyTimestamp(System.currentTimeMillis());
		return hedgeOrder;
	}
	
	/**
	 * 调整订单
	 * @param depth
	 * @return
	 */
	/*
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
	*/
	
	
	
	/*--------------------------------------------------------*/
	
	
	
	@Override
	public void run(Map<String, Object> parameters) {
		
		// 校验参数
		if(!this.validateParameters()){
			logger.error("参数校验失败。");
			return;
		}
		
		// 初始化策略
		this.init();
		
		// 加载未完成的订单
		this.loadUnfilledOrders();
		
		// 个更新余额
		while(true){
			if(this.updateBalance()){
				break;
			}
			logger.error("更新余额失败。");
			TimeUtil.delay(failedSleepTime);
		}
		
		int cycleTimes = 0;
		
		while(true){
			TimeUtil.delay(this.cycle);
			logger.debug("新一轮。");
			
			
			if(cycleTimes++>9999){
				cycleTimes = 0;
			}
			
			
			Depth depth = exchange.getDepth(currencyPair);
			logger.debug(depth);
			if(depth == null){
				logger.error("获取深度信息失败，{}秒后进行下一轮操作。", this.failedSleepTime / 1000);
				TimeUtil.delay(this.failedSleepTime);
				continue;
			}
			
			// 更新订单
			if(cycleTimes % this.updateOrderFrequency == 0 ||
					depth.getBids().get(0).getPrice().compareTo(this.unfilledOrders.get(this.unfilledOrders.size()-1).getOrderPrice())>=0 ||
					depth.getAsks().get(0).getPrice().compareTo(this.unfilledOrders.get(0).getOrderPrice()) <= 0){
				// 更新订单信息
				while(!this.updateOrders()){
					logger.error("更新订单信息失败，{}秒后重试。", this.failedSleepTime / 1000);
					TimeUtil.delay(this.failedSleepTime);
					continue;
				}
			}
			
			this.placePlanOrder();
			
			if(!this.placeOrder(depth)){
				logger.error("下单失败，{}秒后进行下一轮操作。", this.failedSleepTime / 1000);
				TimeUtil.delay(failedSleepTime);
				continue;
			}
			
			/*if(!adjustOrder(depth)){
				logger.error("调整订单失败，{}秒后进行下一轮。", this.failedSleepTime / 1000);
				TimeUtil.delay(this.failedSleepTime);
			}*/
			
		}
	}
	
	public static void main(String[] args){
		new WaveHedge()
		.setExchangeName("hadax.com")
		.setCurrencyPair("PNT_ETH")
		.setQuantity(new BigDecimal("150"))
		.setFeeRate(new BigDecimal("0.002"))
		.setHighestPrice(new BigDecimal("0.001"))
		.setLowestPrice(new BigDecimal("0.000005"))
		.setPriceScale(8)
		.setQuantityScale(2)
		.setMinQuantity(new BigDecimal("100"))
		.setIntervalRate(new BigDecimal("0.005"))
		.setMinMarginRate(new BigDecimal("0.009"))
		.setFailedSleepTime(60000L)
		.setCycle(5000L)
		.setUpdateBalanceFrequency(10000)
		.setUpdateOrderFrequency(12)
		.run(null);
	}

}
