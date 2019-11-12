package quant.strategy;

import exunion.exchange.Exchange;
import exunion.metaobjects.CurrencyPair;
import exunion.metaobjects.Depth;
import exunion.metaobjects.Order;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import quant.exchange.EndExchangeFactory;
import quant.utils.BigDecimalUtil;
import quant.utils.TimeUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Wool implements Strategy {

    private CurrencyPair currencyPair;

    private Depth depth;

    private Exchange exchange;

    private BigDecimal maxQuantity;

    private BigDecimal minQuantity;

    private Logger logger = LogManager.getLogger(Wool.class);

    private List<Order> caclOrders(BigDecimal buy1Price){
        List<Order> result = new ArrayList<>();

        BigDecimal k = BigDecimal.ONE;
        for(int i=0; i<currencyPair.getPriceScale(); i++){
            k = k.multiply(BigDecimal.TEN);
        }

        BigDecimal minPricision = BigDecimal.ONE.divide(k, currencyPair.getPriceScale(), RoundingMode.DOWN);

        BigDecimal order1Price = buy1Price.add(minPricision);
        BigDecimal order2Price = order1Price.add(minPricision);
        Order buyOrder = new Order();
        Order sellOrder = new Order();
        buyOrder.setPrice(order1Price);
        sellOrder.setPrice(order2Price);

        BigDecimal quantity = BigDecimalUtil.randomBigdecimal(minQuantity, maxQuantity, currencyPair.getQuantityScale());

        buyOrder.setQuantity(quantity);
        sellOrder.setQuantity(quantity);
        buyOrder.setSide("BUY");
        sellOrder.setSide("SELL");

        result.add(buyOrder);
        result.add(sellOrder);

        result.parallelStream().forEach(e ->{
            Order od;
            if(( od = exchange.order(e.getSide(), currencyPair.getCurrencyPair(), e.getQuantity(), e.getPrice()))!=null){
                e.setOrderId(od.getOrderId());
                e.setStatus("NEW");
                logger.info("订单{} {} 量：{}, 价格：{}, 编号：{} 下单成功！", e.getSide(), currencyPair.getCurrencyPair(), e.getQuantity(), e.getPrice(), e.getOrderId());
            }else {
                System.exit(0);
            }
        } );
        return result;
    }


    private void isFinished(List<Order> orders){

        Boolean isFinish = true;


        while (true){
            isFinish = true;
            for(Order order : orders){
                if("NEW".equals(order.getStatus())){
                    Order od = exchange.getOrder(currencyPair.getCurrencyPair(), order.getOrderId());
                    if("NEW".equals(od.getStatus())){
                        isFinish = false;
                        break;
                    }else{
                        logger.info("订单 {} {} 价格：{}, 量：{} 已成交！", order.getSide(), currencyPair.getCurrencyPair(), order.getPrice(), order.getQuantity());
                    }
                }
            }
            if (isFinish){
                break;
            }
            TimeUtil.delay(1000L);
        }
    }

    @Override
    public void run(Map<String, Object> parameters) {

        currencyPair = new CurrencyPair("TAT", "ETH", 8, 2);
        currencyPair.setBuyFeeRate(new BigDecimal("0"));
        currencyPair.setSellFeeRate(new BigDecimal("0.002"));

        exchange = EndExchangeFactory.newInstance("fcoin.com");

        minQuantity = new BigDecimal("100");
        maxQuantity = new BigDecimal("500");


        while (true){
            depth = exchange.getDepth(currencyPair.getCurrencyPair());
            BigDecimal buy1Price = depth.getBids().get(0).getPrice().stripTrailingZeros();
            List<Order> orders = caclOrders(buy1Price);
            isFinished(orders);
        }
    }

    public static void main(String[] args){
        new Wool().run(null);
    }

}
