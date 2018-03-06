package quant.strategy;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import quant.entity.LiveOrderPair;
import quant.utils.HibernateUtil;
import traderobot.metaobjects.Depth;
import traderobot.metaobjects.Order;
import traderobot.metaobjects.OrderSide;
import traderobot.metaobjects.OrderStatus;
import traderobot.metaobjects.Ticker;
import traderobot.trade.Tradable;
import traderobot.trade.TraderFactory;

public class SimpleHedge {
	
	private static final Logger logger = LogManager.getLogger("SimpleHedgeLogger");
	
	//交易平台
	private String plantform = null;
	
	private Tradable trader = null;
	
	//API操作失败休息时间， （单位：毫秒），默认 1000ms
	private Long failedSleepTime = 1000L;
	
	//最高操作价格
	private BigDecimal maxPrice = null;
	
	//最低操作价格
	private BigDecimal minPrice = null;
	
	//单次操作量
	private BigDecimal quantity = null;
	
	//对冲交易对之间的价格比率间隔
	private BigDecimal intervalRate = null;
	
	//交易对最小利润率，刨除交易费率之后的利润率
	private BigDecimal minProfitMargin = null;
	
	//交易对最大利润率，刨除交易费率之后的最大利润率
	private BigDecimal maxProfitMargin = null;
	
	//交易费率
	private BigDecimal feeRate = null;

	//价格精度，小数点后 n 位
	private int pricePrecision = 2;
	
	//操作的币种
	private String currency = null;
	
	//操作周期, 默认10毫秒
	private Long cycle = 10L;
	
	private List<LiveOrderPair> liveSellOrderPairs = null;
	
	private List<LiveOrderPair> liveBuyOrderPairs = null;
	
	private List<LiveOrderPair> planSellOrderPairs = null;
	
	private List<LiveOrderPair> planBuyOrderPairs = null;
	
	//排序规则
	//private Comparator<LiveOrderPair> orderByBuyPriceAsc = null;
	
	private Comparator<LiveOrderPair> orderByBuyPriceDesc = null;
	
	private Comparator<LiveOrderPair> orderBySellPriceAsc = null;
	
	//private Comparator<LiveOrderPair> orderBySellPriceDesc = null;
	
	private BigDecimal kM = null;
	
	private BigDecimal kN = null;
	
	BigDecimal ONE = new BigDecimal("1");
	
	/**
	 * 验证参数的合理性，验证失败时打印验证错误原因，并退出系统，减少不必要的损失。
	 */
	private Boolean validateParameters(){
		
		Boolean result = true;
		
		if(null == plantform){
			logger.error("交易平台不能为空。");
			result = false;
		}
		
		if(null == maxPrice){
			logger.error("最高操作价格不能为空，请调用本类的 setMaxPrice 方法设置。");
			result = false;
		}
		
		if(null == minPrice){
			logger.error("最低操作价格不能为空，请调用本类 setMinPrice 方法设置。");
			result = false;
		}
		
		if(null == quantity){
			logger.error("单笔交易操作量不能为空， 请调用本类 setQuantity 方法设置。");
			result = false;
		}
		
		if(null == intervalRate){
			logger.error("对冲交易对之间的比率间隔不能为空，请调用本类 setIntervalRate 方法设置。");
			result = false;
		}
		
		if(null == minProfitMargin){
			logger.error("最低利润率（刨除交易费率）不能为空，请调用本类的 setMinProfitMargin 方法设置。");
			result = false;
		}
		
		if(null == maxProfitMargin){
			logger.warn("最高利润率（刨除交易费率）不能为空，请调用本类的 setMaxProfitMargin 方法设置。");
		}
		
		if(null == currency){
			logger.error("币种不能为空，请调用本类的 setCurrency 方法设置");
			result = false;
		}
		
		if(null == feeRate){
			logger.error("交易费率不能为空。");
			result = false;
		}
		
		Depth depth = trader.getDepth(currency);
		if(depth == null){
			logger.error("获取深度信息失败。");
			result = false;
		}
		
		if(false == result){
			return false;
		}
		
		BigDecimal bidPrice = depth.getBids().get(0).getPrice();
		if(minPrice.compareTo(bidPrice) >= 0){
			logger.error("最低操作价不能高于当前的买一价。");
			result = false;
		}
		
		BigDecimal askPrice = depth.getAsks().get(0).getPrice();
		if(this.maxPrice.compareTo(askPrice) <= 0){
			logger.error("最高操作价不能低于当前的卖一价。");
			result = false;
		}
		
		if(maxProfitMargin.compareTo(minProfitMargin) < 0){
			logger.error("最高利润率不能低于最低利润率");
			result = false;
		}
		
		if(pricePrecision < 0){
			logger.error("价格精度不能小于 0");
			result = false;
		}
		
		return result;
	}
	
	/**
	 * 初始化相关变量，包括实例化一些对象，计算参数值。
	 */
	private void init(){
		if(null == trader){
			trader = TraderFactory.newInstance(plantform);
		}
		
		if(null == liveSellOrderPairs){
			liveSellOrderPairs = new ArrayList<LiveOrderPair>();
		}
		
		if(null == liveBuyOrderPairs){
			liveBuyOrderPairs = new ArrayList<LiveOrderPair>();
		}
		
		if(null == planBuyOrderPairs){
			planBuyOrderPairs = new ArrayList<LiveOrderPair>();
		}
		
		if(null == planSellOrderPairs){
			planSellOrderPairs = new ArrayList<LiveOrderPair>();
		}
		
		if(null == kM){
			BigDecimal wholeMaxProfitMargin = maxProfitMargin.add(feeRate).add(feeRate.multiply(ONE.add(feeRate))); 
			kM = new BigDecimal(StrictMath.sqrt(wholeMaxProfitMargin.add(ONE).doubleValue())).divide(ONE, 4, RoundingMode.HALF_EVEN);
		}
		
		if(null == kN){
			BigDecimal wholeMinProfitMargin = minProfitMargin.add(feeRate).add(feeRate.multiply(ONE.add(feeRate)));
			kN = new BigDecimal(StrictMath.sqrt(wholeMinProfitMargin.add(ONE).doubleValue())).divide(ONE, 4, RoundingMode.HALF_EVEN);
		}
		
		orderByBuyPriceDesc = new Comparator<LiveOrderPair>() {
			public int compare(quant.entity.LiveOrderPair o1, quant.entity.LiveOrderPair o2) {
				return o2.getBuyOrderPrice().compareTo(o1.getBuyOrderPrice());
			}
		};
		
		orderBySellPriceAsc = new Comparator<LiveOrderPair>() {
			public int compare(quant.entity.LiveOrderPair o1, quant.entity.LiveOrderPair o2) {
				return o1.getBuyOrderPrice().compareTo(o2.getBuyOrderPrice());
			}
		};
		
	}
	
	
	
	
	
	/**
	 * 同步订单信息。
	 * 
	 * <p>在策略初始启动的时候执行，执行之后内存中各对象的状态如下：
	 * <ul>
	 * <li> 进行中的买单列表 liveBuyOrderPairs 中仅包含进行中的买单
	 * <li> 进行中的卖单列表 liveSellOrderPairs 中仅包含进行中的卖单
	 * <li> 计划买单交易对列表 planBuyOrderPairs 中仅包含买单状态为 PLAN 的交易对，卖单状态不限
	 * <li> 计划卖单交易对列表 planSellOrderPairs 中仅包含卖单状态为 PLAN 的交易对，买单状态不限
	 * 
	 */
	private Boolean updateOrders(){
		Session session = HibernateUtil.getSession();
		
		//检索没有完全完成的对冲交易对
		String hql = "from LiveOrderPair where currency=:currency and (buyOrderStatus!='FILLED' or sellOrderStatus!='FILLED')";
		Query<LiveOrderPair> query = session.createQuery(hql, LiveOrderPair.class);
		query.setParameter("currency", currency);
		List <LiveOrderPair> persistedOrderPairs = query.getResultList();
		
		for(LiveOrderPair orderPair : persistedOrderPairs){
			
			String buyOrderStatus = orderPair.getBuyOrderStatus();
			
			String sellOrderStatus = orderPair.getSellOrderStatus();
			
			//将不同状态的订单放到不同的列表当中
			
			if("PLAN".equals(buyOrderStatus)){
				planBuyOrderPairs.add(orderPair);
			}else if(OrderStatus.NEW.equals(buyOrderStatus)){
				liveBuyOrderPairs.add(orderPair);
			}
			
			if("PLAN".equals(sellOrderStatus)){
				planSellOrderPairs.add(orderPair);
			}else if(OrderStatus.NEW.equals(sellOrderStatus)){
				liveSellOrderPairs.add(orderPair);
			}
			
		}
		logger.info("订单从数据库加载完成！");
		logger.info("进行中买单数：" + liveBuyOrderPairs.size());
		logger.info("进行中卖单数：" + liveSellOrderPairs.size());
		logger.info("计划中买单数：" + planBuyOrderPairs.size());
		logger.info("计划中卖单数：" + planSellOrderPairs.size());
		return true;
	}
	
	/**
	 * 根据深度和行情信息为设计对冲交易对的价格。
	 * @param depth 深度信息
	 * @param ticker 行情信息
	 * @param buyPrice 买单价格
	 * @param sellPrice 卖单价格
	 */
	private void calculatePrice(Depth depth, Ticker ticker,final PricePair pricePair){
		
		BigDecimal lastPrice = ticker.getLastPrice();
		BigDecimal buy1Price = depth.getBids().get(0).getPrice();	//	买一价
		BigDecimal sell1Price = depth.getAsks().get(0).getPrice();	//	卖一价
		
		//总费率，基于买价 
		// 总费率 = 一般费率 + 一般费率 * (卖出价 ÷ 买入价)
		BigDecimal wholeFeeRate = feeRate.add(feeRate.multiply(sell1Price.divide(buy1Price, 6, RoundingMode.HALF_EVEN))); 
		
		//利润率，基于买价
		BigDecimal profitMargin = sell1Price.subtract(buy1Price).divide(buy1Price, 6, RoundingMode.HALF_EVEN).subtract(wholeFeeRate);
		
		BigDecimal _buyPrice = null;
		BigDecimal _sellPrice = null;
		
		//以买一价买，卖一价卖的利润率大于最小利润率，小于最大利润率
		if(profitMargin.compareTo(minProfitMargin)>=0 && profitMargin.compareTo(maxProfitMargin)<=0){
			_buyPrice = buy1Price;	//直接以买一价作为买价，卖一价作为卖价
			_sellPrice = sell1Price;
		//以买一价买，卖一价卖的利润率小于最小利润率，根据最小利润率和最新成交价调整买入卖出价格
		}else if(profitMargin.compareTo(minProfitMargin)<0){
			_buyPrice = lastPrice.divide(kN, pricePrecision, RoundingMode.HALF_DOWN);
			_sellPrice = lastPrice.multiply(kN).divide(ONE, pricePrecision, RoundingMode.HALF_DOWN);
		//以买一价买，卖一价卖的利润率大于最大利润率，根据最大利润率和最新成交价调整买入卖出价格
		}else if(profitMargin.compareTo(maxProfitMargin)>0){
			_buyPrice = lastPrice.divide(kM, pricePrecision, RoundingMode.HALF_DOWN);
			_sellPrice = lastPrice.multiply(kM).divide(ONE, pricePrecision, RoundingMode.HALF_DOWN);
		}
		pricePair.setBuyPrcie(_buyPrice);
		pricePair.setSellPrice(_sellPrice);
	}
	
	//获取最靠近两个单子的金额
	private void closestPrice(final PricePair pricePair){
		
		BigDecimal _maxBuyPrice = new BigDecimal(-1);
		BigDecimal _minSellPrice = new BigDecimal(-1);
		
		//求最大的买单价格
		if(liveBuyOrderPairs.size() == 0){
			_maxBuyPrice = minPrice;
		}else{
			Collections.sort(liveBuyOrderPairs, new Comparator<LiveOrderPair>() {
				public int compare(LiveOrderPair o1, LiveOrderPair o2) {
					return o2.getBuyOrderPrice().compareTo(o1.getBuyOrderPrice());//从大到小排序
				}
			});
			_maxBuyPrice = liveBuyOrderPairs.get(0).getBuyOrderPrice();
		}
		
		//求最小的卖单价格
		if(liveSellOrderPairs.size() == 0){
			_minSellPrice = maxPrice;
		}else{
			Collections.sort(liveSellOrderPairs, orderBySellPriceAsc);
			_minSellPrice = liveSellOrderPairs.get(0).getSellOrderPrice();
		}
		pricePair.setBuyPrcie(_maxBuyPrice);
		pricePair.setSellPrice(_minSellPrice);
	}
	
	/**
	 * 判断是否满足下普通买单的条件，若买入价与当前账户进行买单中最高买价相差超过订单间隔比率 intervalRate，
	 * 则表示满足买入条件，否则不满足。
	 * @param buyPrice 买入价
	 * @param maxBuyPrice 当前账户进行订单中买入价格最高买单
	 * @return true - 满足买入条件， false - 不满足条件
	 */
	private Boolean isMeetBuyOrderCondition(BigDecimal buyPrice, BigDecimal maxBuyPrice){
		return buyPrice.subtract(maxBuyPrice).divide(maxBuyPrice, 4, RoundingMode.HALF_EVEN).compareTo(intervalRate) >= 0;
	}
	
	/**
	 * 判断是否满足下计划买单的条件。 当计划买单中的最高价满足下普通买单条件或者计划买单中的最高价大于卖一价时，
	 * 表示满足下计划买单的条件。
	 * <p>采用这种方式判断的目的在于尽快完成对冲订单对。
	 * @param buyPrice 计划买单中的最高价
	 * @param maxBuyPrice 当前账户进行买单中的最高买价 
	 * @param depth 市场深度信息
	 * @return true - 满足买入条件， false - 不满足买入条件
	 */
	private Boolean isMeetPlanBuyOrderCondition(BigDecimal buyPrice, BigDecimal maxBuyPrice, Depth depth){
		return isMeetBuyOrderCondition(buyPrice, maxBuyPrice) 
				|| buyPrice.compareTo(depth.getAsks().get(0).getPrice()) > 0;
	}
	
	//判断是否满足下计划卖单的条件
	private Boolean isMeetPlanSellOrderCondition(BigDecimal sellPrice, BigDecimal minSellPrice, Depth depth){
		return isMeetSellOrderCondition(sellPrice, minSellPrice)
				|| sellPrice.compareTo(depth.getBids().get(0).getPrice()) < 0;
	}
	
	//判断是否满足下卖单的条件
	private Boolean isMeetSellOrderCondition(BigDecimal sellPrice, BigDecimal minSellPrice){
		return minSellPrice.subtract(sellPrice).divide(sellPrice, 4, RoundingMode.HALF_EVEN).compareTo(intervalRate) >= 0;
	}
	
	
	/**
	 * 
	 * <P> 每轮都执行此方法，用于实时同步订单信息。
	 * 
	 * <P> 更新已经下单的订单信息, 若更新时出现异常情况，则返回 null
	 * 
	 * <p> 对进行中的买单按照金额大小从大到小排序，逐个判断订单是否成交。
	 * 若订单已经成交，则从进行中的订单列表中移除，同时更新数据库。
	 * 
	 * @return
	 */
	private Boolean updateLiveOrders(){
		
		Collections.sort(liveBuyOrderPairs, orderByBuyPriceDesc);
		int primitiveBuyOrderSize = liveBuyOrderPairs.size();
		for(int i=0, k=0; i<primitiveBuyOrderSize - k; i++){
			Order order = trader.getOrder(liveBuyOrderPairs.get(i).getCurrency(), liveBuyOrderPairs.get(i).getBuyOrderId());
			if(order == null){
				logger.error("获取买单" + liveBuyOrderPairs.get(i).getBuyOrderId() + "失败！");
				return false;
			}
			LiveOrderPair orderPairs = liveBuyOrderPairs.get(i);
			Boolean isStatusChanged = false;
			//订单已经成交
			if(order.getStatus().equals(OrderStatus.FILLED)){
				orderPairs.setBuyOrderPrice(order.getPrice());
				orderPairs.setBuyOrderStatus(order.getStatus());
				orderPairs.setBuyOrderQuantity(order.getQuantity());
				isStatusChanged = true;
			}else if (order.getStatus().equals(OrderStatus.CANCELED)){//订单已被取消
				//订单不能被取消，被取消的话对冲实现起来困难，将被取消的订单放到计划订单列表中
				orderPairs.setBuyOrderStatus("PLAN");
				planBuyOrderPairs.add(orderPairs);
				isStatusChanged = true;
			}//因为金额大的买单都没有成交，下面金额小的更不可能成交，所以直接跳出，不必再查看是否成交。
			
			if(isStatusChanged == true){
				logger.info("订单编号为[" + orderPairs.getBuyOrderId() + "]的买单状态变为" + orderPairs.getBuyOrderStatus());
				liveBuyOrderPairs.remove(i);
				k++;
				i--;
				//更新数据库
				Session session = HibernateUtil.getSession();
				Transaction transaction = session.beginTransaction();
				session.merge(orderPairs);
				
				transaction.commit();
				session.close();
			}else{
				break;
			}
		}
		
		Collections.sort(liveSellOrderPairs, orderBySellPriceAsc);
		int primitiveSellOrderSize = liveSellOrderPairs.size();
		for(int i=0, k=0; i<primitiveSellOrderSize - k; i++){
			Order order = trader.getOrder(liveSellOrderPairs.get(i).getCurrency(), liveSellOrderPairs.get(i).getSellOrderId());
			LiveOrderPair orderPair = liveSellOrderPairs.get(i);
			Boolean isStatusChanged = false;
			if(null == order ){
				logger.error("获取卖单" + liveSellOrderPairs.get(i).getSellOrderId() + "失败！");
				return false;
			}else{
				if(order.getStatus().equals(OrderStatus.FILLED)){
					orderPair.setSellOrderStatus(order.getStatus());
					orderPair.setSellOrderPrice(order.getPrice());
					orderPair.setSellOrderQuantity(order.getQuantity());
					liveSellOrderPairs.remove(i);
					isStatusChanged = true;
				}else if(order.getStatus().equals(OrderStatus.CANCELED)){
					orderPair.setSellOrderStatus("PLAN");
					planSellOrderPairs.add(orderPair);
					isStatusChanged = true;
				}
			}
			
			if(isStatusChanged){
				logger.info("订单编号为[" + orderPair.getSellOrderId() + "]的买单状态变为" + orderPair.getSellOrderStatus());
				k++;
				i--;
				//更新数据库
				Session session = HibernateUtil.getSession();
				Transaction transaction = session.beginTransaction();
				session.merge(orderPair);
				transaction.commit();
				session.close();
			}else{//金额小的卖单状态没有改变，金额大的卖单更不可能成交
				break;
			}
		}
		return true;
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
	
	public String getPlantform() {
		return plantform;
	}

	public SimpleHedge setPlantform(String plantform) {
		this.plantform = plantform;
		return this;
	}

	public String getCurrency() {
		return currency;
	}

	public SimpleHedge setCurrency(String currency) {
		this.currency = currency;
		return this;
	}

	public Long getCycle() {
		return cycle;
	}

	public SimpleHedge setCycle(Long cycle) {
		this.cycle = cycle;
		return this;
	}
	
	public Tradable getTrader() {
		return trader;
	}

	public BigDecimal getMaxPrice() {
		return maxPrice;
	}

	public SimpleHedge setMaxPrice(BigDecimal maxPrice) {
		this.maxPrice = maxPrice;
		return this;
	}

	public BigDecimal getMinPrice() {
		return minPrice;
	}

	public SimpleHedge setMinPrice(BigDecimal minPrice) {
		this.minPrice = minPrice;
		return this;
	}

	public BigDecimal getQuantity() {
		return quantity;
	}

	public SimpleHedge setQuantity(BigDecimal quantity) {
		this.quantity = quantity;
		return this;
	}

	public BigDecimal getIntervalRate() {
		return intervalRate;
	}

	public SimpleHedge setIntervalRate(BigDecimal intervalRate) {
		this.intervalRate = intervalRate;
		return this;
	}

	public BigDecimal getMinProfitMargin() {
		return minProfitMargin;
	}

	public SimpleHedge setMinProfitMargin(BigDecimal minProfitMargin) {
		this.minProfitMargin = minProfitMargin;
		return this;
	}

	public BigDecimal getMaxProfitMargin() {
		return maxProfitMargin;
	}

	public SimpleHedge setMaxProfitMargin(BigDecimal maxProfitMargin) {
		this.maxProfitMargin = maxProfitMargin;
		return this;
	}

	public BigDecimal getFeeRate() {
		return feeRate;
	}

	public SimpleHedge setFeeRate(BigDecimal feeRate) {
		this.feeRate = feeRate;
		return this;
	}

	//获取价格精度，小数点后n位
	public int getPricePrecision() {
		return pricePrecision;
	}

	public Long getFailedSleepTime() {
		return failedSleepTime;
	}

	//设置API失败之后的休息时间（单位：毫秒），默认1000ms
	public SimpleHedge setFailedSleepTime(Long failedSleepTime) {
		this.failedSleepTime = failedSleepTime;
		return this;
	}

	/**
	 * 设置价格精度，小数点后 pricePrecision 位。
	 * @param pricePrecision 默认值： 2
	 * @return
	 */
	
	public SimpleHedge setPricePrecision(int pricePrecision) {
		this.pricePrecision = pricePrecision;
		return this;
	}
	
	private static class PricePair{
		
		private BigDecimal buyPrcie = null;
		
		private BigDecimal sellPrice = null;
		
		public PricePair(){}
		
		public BigDecimal getBuyPrcie() {
			return buyPrcie;
		}

		public void setBuyPrcie(BigDecimal buyPrcie) {
			this.buyPrcie = buyPrcie;
		}

		public BigDecimal getSellPrice() {
			return sellPrice;
		}

		public void setSellPrice(BigDecimal sellPrice) {
			this.sellPrice = sellPrice;
		}
	}
	
	
	/**
	 * 交易入口，调用此方法之后系统将按照策略持续下单，更新订单信息。
	 */
	public void earnMoney(){

		//初始化对象
		this.init();
		
		//校验参数
		if(!this.validateParameters()){
			logger.fatal("参数校验失败！退出系统。");
			System.exit(0);
		}
		//同步订单信息
		this.updateOrders();
		
		while(true){
			
			delay(cycle);
			logger.debug("新一轮");
			//更新挂单信息
			if(!updateLiveOrders()){
				logger.error("更新进行中的订单信息失败！");
				logger.info("系统将在" + failedSleepTime/1000 + "秒后重试。");
				delay(failedSleepTime);
				logger.info("系统即将重试！");
				continue;
			}
			
			Depth depth = trader.getDepth(currency);
			//若获取深度信息失败，则暂停一段时间后再继续
			if(depth == null){
				logger.info("系统将在" + failedSleepTime/1000 + "秒后重试。");
				delay(failedSleepTime);
				logger.info("系统即将重试！");
				continue;
			}
			
			Ticker ticker = trader.getTicker(currency);
			if(null == ticker){
				logger.info("获取" + currency + "的行情信息失败！");
				logger.info("系统将在" + failedSleepTime/1000 + "秒后重试。");
				delay(failedSleepTime);
				logger.info("系统即将重试！");
				continue;
			}
			
			PricePair pricePair = new PricePair();
			
			// 计算进行中的订单中的最高买价和最低卖价
			BigDecimal maxBuyPrice = null;
			BigDecimal minSellPrice = null;
			closestPrice(pricePair);
			maxBuyPrice = pricePair.getBuyPrcie();
			minSellPrice = pricePair.getSellPrice();

			//计算买入价与卖出价
			BigDecimal buyPrice = null;	//买入价
			BigDecimal sellPrice = null;//卖出价

			
			Boolean isBuy = false; //标志此轮是否下了买单
			Boolean isSell = false; //标志此轮是否下了卖单
			
			//优先以计划订单中的买入价格的最高价和卖出价格的最低价作为买入价和卖出价
			//存在计划的买单
			if(planBuyOrderPairs.size()>0){
				Collections.sort(planBuyOrderPairs, new Comparator<LiveOrderPair>() {
					public int compare(LiveOrderPair o1, LiveOrderPair o2) {
						return o1.getSellOrderStatus().compareTo(o2.getSellOrderStatus()) == 0 ?
								o2.getBuyOrderPrice().compareTo(o1.getBuyOrderPrice()) : 
									o1.getSellOrderStatus().compareTo(o2.getSellOrderStatus());
					}
				});
				LiveOrderPair maxBuyPricePlanBuyOrderPair = planBuyOrderPairs.get(0);//计划买入订单中买入价最高的订单
				buyPrice = maxBuyPricePlanBuyOrderPair.getBuyOrderPrice();
				if(isMeetPlanBuyOrderCondition(buyPrice, maxBuyPrice, depth)){//满足下计划买单的条件
					Order order = trader.order(OrderSide.BUY, maxBuyPricePlanBuyOrderPair.getCurrency(), 
							maxBuyPricePlanBuyOrderPair.getBuyOrderQuantity() , buyPrice);
					if(null != order){//下单成功
						isBuy = true;
						maxBuyPricePlanBuyOrderPair.setBuyOrderId(order.getOrderId());
						maxBuyPricePlanBuyOrderPair.setBuyOrderStatus(order.getStatus());
						liveBuyOrderPairs.add(maxBuyPricePlanBuyOrderPair);//加入到进行中买单列表
						planBuyOrderPairs.remove(0);//从计划买单列表中移除
						Session session = HibernateUtil.getSession();//同步数据库中的状态
						session.beginTransaction();
						session.merge(maxBuyPricePlanBuyOrderPair);
						session.getTransaction().commit();
					}else{
						logger.debug("下计划买单失败！");
						//delay(1000L);
						//continue;
					}
				}
			}
			
			if(planSellOrderPairs.size()>0){
				Collections.sort(planSellOrderPairs, new Comparator<LiveOrderPair>() {
					public int compare(LiveOrderPair o1, LiveOrderPair o2) {
						return o1.getBuyOrderStatus().compareTo(o2.getBuyOrderStatus()) == 0 ?
								o1.getSellOrderPrice().compareTo(o2.getSellOrderPrice()) :
									o1.getBuyOrderStatus().compareTo(o2.getBuyOrderStatus());
					}
				});
				LiveOrderPair minSellPricePlanSellOrderPair = planSellOrderPairs.get(0);
				sellPrice = minSellPricePlanSellOrderPair.getSellOrderPrice();
				if(isMeetPlanSellOrderCondition(sellPrice, minSellPrice, depth)){//满足下计划卖单的条件
					Order order = trader.order(OrderSide.SELL, minSellPricePlanSellOrderPair.getCurrency(),
							minSellPricePlanSellOrderPair.getSellOrderQuantity(), sellPrice);
					if(null != order){
						isSell = true;
						minSellPricePlanSellOrderPair.setSellOrderId(order.getOrderId());
						minSellPricePlanSellOrderPair.setSellOrderStatus(order.getStatus());
						liveSellOrderPairs.add(minSellPricePlanSellOrderPair);
						planSellOrderPairs.remove(0);
						Session session = HibernateUtil.getSession();
						session.beginTransaction();
						session.merge(minSellPricePlanSellOrderPair);
						session.getTransaction().commit();
					}else{
						logger.error("下计划卖单失败！");
					}
				}
			}
			
			if(isBuy || isSell){//此轮已经下单了
				continue;
			}
			
			// 根据行情来计算买单和卖单价格
			this.calculatePrice(depth, ticker, pricePair);
			buyPrice = pricePair.getBuyPrcie();
			sellPrice = pricePair.getSellPrice();
			
			LiveOrderPair orderPair = new LiveOrderPair();
			orderPair.setCreateTimestamp(System.currentTimeMillis());
			orderPair.setModifyTimestamp(System.currentTimeMillis());
			orderPair.setCurrency(currency);
			orderPair.setPlantform(plantform);
			orderPair.setBuyOrderPrice(buyPrice);
			orderPair.setBuyOrderQuantity(quantity);
			orderPair.setSellOrderPrice(sellPrice);
			orderPair.setSellOrderQuantity(quantity);
			
			if(isMeetBuyOrderCondition(buyPrice, maxBuyPrice)){
				Order buyOrder = trader.order("BUY", currency, quantity, buyPrice);
				if(null == buyOrder){
					logger.debug("买单下单失败。");
					isBuy = false;
					//continue;
				}else{
					//买单下单成功
					liveBuyOrderPairs.add(orderPair);
					orderPair.setBuyOrderStatus(buyOrder.getStatus());
					orderPair.setBuyOrderId(buyOrder.getOrderId());
					isBuy = true;
				}
			}else{
				isBuy = false;
			}
			
			if(isMeetSellOrderCondition(sellPrice, minSellPrice)){
				Order sellOrder = trader.order("SELL", currency, quantity, sellPrice);
				
				//卖单若下单成功，则更新订单编号和状态，否则将订单设置为计划订单
				if(null == sellOrder){
					logger.debug("卖单下单失败。");
					if(isBuy) {
						logger.debug("买单下单成功，卖单下单失败，卖单将被转化为计划卖单。");
					}
					delay(1000L);
					isSell = false;
				}else{
					liveSellOrderPairs.add(orderPair);
					orderPair.setSellOrderId(sellOrder.getOrderId());
					orderPair.setSellOrderStatus(sellOrder.getStatus());
					isSell = true;
				}
			}else{
				isSell = false;
			}
			
			if(isSell || isBuy){//买单或者卖单下单成功了
				
				if(!isSell){//卖单下单失败
					orderPair.setSellOrderStatus("PLAN");
					planSellOrderPairs.add(orderPair);
				}else if(!isBuy){//买单下单失败
					orderPair.setBuyOrderStatus("PLAN");
					planBuyOrderPairs.add(orderPair);
				}
				
				//保存到数据库
				Session session = HibernateUtil.getSession();
				Transaction tx = session.beginTransaction();
				session.save(orderPair);
				tx.commit();
				session.close();
			}			
		}
		
	}
	
	public static void main(String[] args){
		SimpleHedge hedge = new SimpleHedge();
		hedge
		.setFeeRate(new BigDecimal("0.002"))
		.setCurrency("HSR_QC")
		.setCycle(1000L)
		.setIntervalRate(new BigDecimal("0.008"))
		.setMaxPrice(new BigDecimal("150"))
		.setMinPrice(new BigDecimal("0.45"))
		.setMaxProfitMargin(new BigDecimal("0.05"))
		.setMinProfitMargin(new BigDecimal("0.01"))
		.setPlantform("zb.com")
		.setQuantity(new BigDecimal("0.01"))
		.setPricePrecision(2)
		.setFailedSleepTime(30000L);;
		hedge.earnMoney();
	}
	
}
