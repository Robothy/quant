package quant.strategy;

import java.math.BigDecimal;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import traderobot.metaobjects.Depth;
import traderobot.metaobjects.Order;
import traderobot.trade.Tradable;

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

	//操作的币种
	private String currency = null;
	
	private Long cycle = null;
	
	private List<Order> orders = null;

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
		
		return result;
	}
	
	
	public void earnMoney(){
		
		this.validateParameters();
		
		while(true){
			;
		}
	}
	
	//同步订单
	private void syncOrders(){
		;
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

	public SimpleHedge setTrader(Tradable trader) {
		this.trader = trader;
		return this;
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
	
}
