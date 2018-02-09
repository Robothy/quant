package quant.strategy;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.criteria.internal.expression.SearchedCaseExpression;

import quant.dao.LiveOrderPairsHome;
import quant.entity.LiveOrderPairs;
import quant.utils.HibernateUtil;
import traderobot.metaobjects.Depth;
import traderobot.metaobjects.Order;
import traderobot.metaobjects.Ticker;
import traderobot.trade.Tradable;
import traderobot.trade.TraderFactory;

public class SimpleHedge {
	
	private static final Logger logger = LogManager.getLogger(SimpleHedge.class);
	
	//交易平台
	private String plantform = null;
	
	private Tradable trader = null;
	
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
	
	private List<LiveOrderPairs> liveOrderPairs = null;

	private List<Order> liveSellOrders = null;
	
	private List<Order> liveBuyOrders = null;
	
	private List<LiveOrderPairs> planSellOrderPairs = null;
	
	private List<LiveOrderPairs> planBuyOrderPairs = null;
	
	private LiveOrderPairsHome liveOrderPairsHome = null;
	
	private BigDecimal kM = null;
	
	private BigDecimal kN = null;
	
	BigDecimal ONE = new BigDecimal("1");
	
	/**
	 * 验证参数的合理性
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
	
	//初始化相关变量
	private void init(){
		if(null == trader){
			trader = TraderFactory.newInstance(plantform);
		}
		
		if(null == liveOrderPairsHome){
			liveOrderPairsHome = new LiveOrderPairsHome();
		}
		
		if(null == liveSellOrders){
			liveSellOrders = new ArrayList<Order>();
		}
		
		if(null == liveBuyOrders){
			liveBuyOrders = new ArrayList<Order>();
		}
		
		if(null == planBuyOrderPairs){
			planBuyOrderPairs = new ArrayList<LiveOrderPairs>();
		}
		
		if(null == planSellOrderPairs){
			planSellOrderPairs = new ArrayList<LiveOrderPairs>();
		}
		
		kM = new BigDecimal(StrictMath.sqrt(maxProfitMargin.add(ONE).doubleValue())).divide(ONE, 4, RoundingMode.HALF_EVEN);
		
		kN = new BigDecimal(StrictMath.sqrt(minProfitMargin.add(ONE).doubleValue())).divide(ONE, 4, RoundingMode.HALF_EVEN);
		
	}
	
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
			Depth depth = trader.getDepth(currency);
			//若获取深度信息失败，则暂停1分钟后再继续
			if(depth == null){
				delay(60000L);
				continue;
			}
			
			Ticker ticker = trader.getTicker(currency);
			if(null == ticker){
				delay(60000L);
				continue;
			}
			
			PricePair pricePair = new PricePair();
			
			// 计算买入价与卖出价
			BigDecimal buyPrice = null;	//买入价
			BigDecimal sellPrice = null;//卖出价
			this.calculatePrice(depth, ticker, pricePair);
			buyPrice = pricePair.getBuyPrcie();
			sellPrice = pricePair.getSellPrice();
			
			// 计算最接近当前价格的订单价格
			BigDecimal maxBuyPrice = null;
			BigDecimal minSellPrice = null;
			closestPrice(pricePair);
			maxBuyPrice = pricePair.getBuyPrcie();
			minSellPrice = pricePair.getSellPrice();
			
			//不满足下单条件，更新订单信息，并跳过此轮
			if(!isMeetOrderCondition(buyPrice, sellPrice, maxBuyPrice, minSellPrice)){
				updateLiveOrders();
				continue;
			}
			
			//满足条件，下订单对（卖单与卖单），更新订单信息
			Order buyOrder = trader.order("BUY", currency, quantity, buyPrice);
			if(null == buyOrder){
				delay(1000L);
				continue;
			}
			
			//买单下单成功
			liveBuyOrders.add(buyOrder);
			LiveOrderPairs orderPair = new LiveOrderPairs();
			orderPair.setCurrency(currency);
			orderPair.setPlantform(plantform);
			orderPair.setCreateTimestamp(System.currentTimeMillis());
			orderPair.setModifyTimestamp(System.currentTimeMillis());
			
			orderPair.setBuyOrderStatus(buyOrder.getStatus());
			orderPair.setBuyOrderId(buyOrder.getOrderId());
			orderPair.setBuyOrderPrice(buyPrice);
			orderPair.setBuyOrderQuantity(quantity);
			orderPair.setBuyOrderStatus(buyOrder.getStatus());
			
			
			Order sellOrder = trader.order("SELL", currency, quantity, sellPrice);
			orderPair.setSellOrderPrice(sellPrice);
			orderPair.setSellOrderQuantity(quantity);
			//卖单若下单成功，则更新订单编号和状态，否则将订单设置为计划订单
			if(null == sellOrder){
				orderPair.setSellOrderStatus("PLAN");
				planSellOrderPairs.add(orderPair);
			}else{
				liveSellOrders.add(sellOrder);
				orderPair.setSellOrderId(sellOrder.getOrderId());
				orderPair.setSellOrderStatus(sellOrder.getStatus());
			}
			
			//保存到数据库
			Session session = HibernateUtil.getSession();
			Transaction tx = session.beginTransaction();
			session.save(orderPair);
			tx.commit();
			session.close();
		}
		
	}
	
	
	
	/**
	 * 更新下单信息。
	 */
	@SuppressWarnings("unchecked")
	private void updateOrders(){
		Session session = HibernateUtil.getSession();
		
		//检索没有完全完成的对冲交易对
		String hql = "from LiveOrderPairs where currency=:currency and (buyOrderStatus!='FILLED' or sellOrderStatus!='FILLED')";
		Query query = session.createQuery(hql);
		query.setString("currency", currency);
		List <LiveOrderPairs> persistedOrderPairs = query.list();
		for(LiveOrderPairs orderPairs : persistedOrderPairs){
			Boolean buyOrderChanged = false; 
			Boolean sellOrderChanged = false;
			//若买入交易对没有完成，且买入交易并非计划交易
			String buyOrderStatus = orderPairs.getBuyOrderStatus();
			if(!"FILLED".equals(buyOrderStatus) && !"PLAN".equals(buyOrderStatus)){
				Order order = trader.getOrder(currency, orderPairs.getBuyOrderId());
				orderPairs.setBuyOrderPrice(order.getPrice());
				orderPairs.setBuyOrderStatus(order.getStatus());
				liveBuyOrders.add(order);
				buyOrderChanged = true;
			}
			
			String sellOrderStatus = orderPairs.getSellOrderStatus();
			if(!"FILLED".equals(sellOrderStatus) && !"PLAN".equals(sellOrderStatus)){
				Order order = trader.getOrder(currency, orderPairs.getSellOrderId());
				orderPairs.setSellOrderPrice(order.getPrice());
				orderPairs.setSellOrderStatus(order.getStatus());
				liveSellOrders.add(order);
				sellOrderChanged = true;
			}
			
			//将计划下的订单对中包含买单的交易对放入到 planBuyOrderPairs 中
			if("PLAN".equals(buyOrderStatus)){
				planBuyOrderPairs.add(orderPairs);
			}
			
			if("PLAN".equals(sellOrderStatus)){
				planSellOrderPairs.add(orderPairs);
			}
			
			//订单发生变化，更新数据库订单的状态
			if (buyOrderChanged && sellOrderChanged){
				liveOrderPairsHome.merge(orderPairs);
			}
		}
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
		//buyPrice.valueOf(_buyPrice.doubleValue());
		//sellPrice.valueOf(_sellPrice.doubleValue());
		//buyPrice.subtract(buyPrice).add(_buyPrice);
		//sellPrice.subtract(sellPrice).add(_sellPrice);
		pricePair.setBuyPrcie(_buyPrice);
		pricePair.setSellPrice(_sellPrice);
		logger.debug("买入价：" + _buyPrice + "，卖出价：" + _sellPrice);
	}
	
	//获取最靠近两个单子的金额
	private void closestPrice(final PricePair pricePair){
		
		BigDecimal _maxBuyPrice = new BigDecimal(-1);
		BigDecimal _minSellPrice = new BigDecimal(-1);
		
		//求最大的买单价格
		if(liveBuyOrders.size() == 0){
			_maxBuyPrice = minPrice;
		}else{
			Collections.sort(liveBuyOrders, new Comparator<Order>() {
				public int compare(Order o1, Order o2) {
					return o2.getPrice().compareTo(o1.getPrice());//从大到小排序
				}
			});
			_maxBuyPrice = liveBuyOrders.get(0).getPrice();
		}
		
		//求最小的卖单价格
		if(liveSellOrders.size() == 0){
			_minSellPrice = maxPrice;
		}else{
			Collections.sort(liveSellOrders, new Comparator<Order>() {
				public int compare(Order o1, Order o2) {
					return o1.getPrice().compareTo(o2.getPrice());//从小到大排序
				}
			});
			_minSellPrice = liveSellOrders.get(0).getPrice();
		}
		pricePair.setBuyPrcie(_maxBuyPrice);
		pricePair.setSellPrice(_minSellPrice);
	}
	
	//检查是否满足下单条件，即买单间隔与卖单间隔操作最小间隔
	private Boolean isMeetOrderCondition(BigDecimal buyPrice, BigDecimal sellPrice, BigDecimal maxBuyPrice, BigDecimal minSellPrice){
		Boolean meetBuyCondition = buyPrice.subtract(maxBuyPrice).divide(maxBuyPrice, 4, RoundingMode.HALF_EVEN).compareTo(intervalRate) > 0;
		Boolean meetSellCondition = minSellPrice.subtract(sellPrice).divide(sellPrice, 4, RoundingMode.HALF_EVEN).compareTo(intervalRate) > 0;
		return meetBuyCondition && meetSellCondition;
	}
	
	/**
	 * 更新已经下单的订单信息, 若更新时出现异常情况，则返回 null
	 * 
	 * <p>对进行中的买单按照金额大小从大到小排序，逐个判断订单是否成交。
	 * 若订单已经成交，则从进行中的订单列表中移除，同时更新数据库。
	 * 
	 * @return
	 */
	private Boolean updateLiveOrders(){
		
		Collections.sort(liveBuyOrders, new Comparator<Order>() {
			public int compare(Order o1, Order o2) {
				return o2.getPrice().compareTo(o1.getPrice());
			}
		});

		for(int i=0, k=0; i<liveBuyOrders.size() - k; i++){
			Order order = trader.getOrder(liveBuyOrders.get(i).getCurrency(), liveBuyOrders.get(i).getOrderId());
			if(order == null){
				return false;
			}
			//订单已经成交
			if(order.getStatus().equals("FILLED")){
				liveBuyOrders.remove(i);
				k++;
				//更新数据库
				Session session = HibernateUtil.getSession();
				Transaction transaction = session.beginTransaction();
				Query query = session.createQuery("from LiveOrderPairs where buyOrderId=:buyOrderId and currency=:currency");
				query.setParameter("buyOrderId", order.getOrderId());
				query.setParameter("currency", order.getCurrency());
				LiveOrderPairs orderPairs = (LiveOrderPairs) query.list().get(0);
				orderPairs.setBuyOrderPrice(order.getPrice());
				orderPairs.setBuyOrderStatus(order.getStatus());
				orderPairs.setBuyOrderQuantity(order.getQuantity());
				session.persist(orderPairs);
				transaction.commit();
				session.close();
			}else{//因为金额大的买单都没有成交，下面金额小的更不可能成交，所以直接跳出，不必再查看是否成交。
				break;
			}
		}
		
		Collections.sort(liveSellOrders, new Comparator<Order>() {
			public int compare(Order o1, Order o2) {
				return o1.getPrice().compareTo(o2.getPrice());
			}
		});
		for(int i=0, k=0; i<liveSellOrders.size() - k; i++){
			Order order = trader.getOrder(liveBuyOrders.get(i).getCurrency(), liveBuyOrders.get(i).getOrderId());
			if(null == order ){
				return false;
			}else{
				if(order.getStatus().equals("FILLED")){
					liveSellOrders.remove(i);
					k++;
					//更新数据库
					Session session = HibernateUtil.getSession();
					Transaction transaction = session.beginTransaction();
					Query query = session.createQuery("from LiveOrderPairs where sellOrderId=:sellOrderId and currency=:currency");
					query.setParameter("sellOrderId", order.getOrderId());
					query.setParameter("currency", order.getCurrency());
					LiveOrderPairs orderPairs = (LiveOrderPairs) query.list().get(0);
					orderPairs.setSellOrderPrice(order.getPrice());
					orderPairs.setSellOrderStatus(order.getStatus());
					orderPairs.setSellOrderQuantity(order.getQuantity());
					session.persist(orderPairs);
					transaction.commit();
					session.close();
				}else{//金额小的卖单都没有成交，金额大的卖单更不可能成交
					break;
				}
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
		
		public PricePair(BigDecimal buyPrice, BigDecimal sellPrice){
			buyPrice = buyPrice;
			sellPrice = sellPrice;
		}
		
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
	
	public static void main(String[] args){
		SimpleHedge hedge = new SimpleHedge();
		hedge
		.setFeeRate(new BigDecimal("0.002"))
		.setCurrency("HSR_QC")
		.setCycle(100L)
		.setIntervalRate(new BigDecimal("0.03"))
		.setMaxPrice(new BigDecimal("150"))
		.setMinPrice(new BigDecimal("0.45"))
		.setMaxProfitMargin(new BigDecimal("0.05"))
		.setMinProfitMargin(new BigDecimal("0.01"))
		.setPlantform("zb.com")
		.setQuantity(new BigDecimal("0.01"))
		.setPricePrecision(2);
		hedge.earnMoney();
	}
	
}
