package quant.strategy;

import com.sun.org.apache.xpath.internal.operations.Bool;
import exunion.exchange.Exchange;
import exunion.metaobjects.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import quant.dao.CommonDao;
import quant.entity.TriangleArbitrageOrder;
import quant.exchange.EndExchangeFactory;
import quant.utils.BigDecimalUtil;
import quant.utils.TimeUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.Currency;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 三角套利策略
 * 针对于某交易所的某币种
 *
 * @author robothy
 */
public class TriangleArbitrage implements Strategy {

    private static Logger logger = LogManager.getLogger(TriangleArbitrage.class);

    /**
     * 套利模式
     */
    enum ArbitrageMode {

        /**
         * 顺时针套利，与逆时针相对应
         */
        CLOCKWISE,

        /**
         * 逆时针套利，与顺时针相对应
         */
        ANTICLOCKWISE,


        /**
         * 无套利空间
         */
        NONE
    }

    /**
     * 计价币A与基础币B构成的交易币种对
     */
    private CurrencyPair currPairBA;

    /**
     * 设置计价币A与基础币B构成的交易币种对
     */
    public CurrencyPair getCurrPairBA() {
        return currPairBA;
    }

    /**
     * 设置计价币A与基础币B构成的交易币种对
     *
     * @param currPairBA 计价币A与基础币B构成的交易币种对
     */
    public TriangleArbitrage setCurrPairBA(CurrencyPair currPairBA) {
        this.currPairBA = currPairBA;
        return this;
    }

    /**
     * 计价币B与基础币C构成的交易币种对
     */
    private CurrencyPair currPairCB;

    /**
     * 获取计价币B与基础币C构成的交易币种对
     *
     * @return 计价币B与基础币C构成的交易币种对
     */
    public CurrencyPair getCurrPairCB() {
        return currPairCB;
    }

    /**
     * 计价币B与基础币C构成的交易币种对
     *
     * @param currPairCB 计价币B与基础币C构成的交易币种对
     */
    public TriangleArbitrage setCurrPairCB(CurrencyPair currPairCB) {
        this.currPairCB = currPairCB;
        return this;
    }

    /**
     * 计价币A与基础币C构成的交易币种对
     */
    private CurrencyPair currPairCA;

    /**
     * 获取计价币A与基础币C构成的交易币种对
     *
     * @return 计价币A与基础币C构成的交易币种对
     */
    public CurrencyPair getCurrPairCA() {
        return currPairCA;
    }

    /**
     * 设置计价币A与基础币C构成的交易币种对
     *
     * @param currPairCA 设置计价币A与基础币C构成的交易币种对
     */
    public TriangleArbitrage setCurrPairCA(CurrencyPair currPairCA) {
        this.currPairCA = currPairCA;
        return this;
    }

    /**
     * 交易所名称
     */
    private String exchangeName;

    /**
     * 获取交易所名称
     *
     * @return 交易所名称
     */
    public String getExchangeName() {
        return exchangeName;
    }

    /**
     * 设置交易所名称
     *
     * @param exchangeName 交易所名称
     */
    public TriangleArbitrage setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
        return this;
    }

    /**
     * 套利币种，即账户上会增加的币种
     */
    private String arbitrageCurr;

    /**
     * 获取套利币种，即账户上会增加的币种
     *
     * @return 套利币种，即账户上会增加的币种
     */
    public String getArbitrageCurr() {
        return arbitrageCurr;
    }

    /**
     * 设置套利币种，即账户上会增加的币种
     *
     * @param arbitrageCurr 套利币种，即账户上会增加的币种
     */
    public TriangleArbitrage setArbitrageCurr(String arbitrageCurr) {
        this.arbitrageCurr = arbitrageCurr;
        return this;
    }

    /**
     * API操作失败休息时间， （单位：毫秒）
     */
    private Long failedSleepTime;

    /**
     * @return 操作失败时休息时间(单位 ： 毫秒)
     */
    public Long getFailedSleepTime() {
        return failedSleepTime;
    }

    /**
     * 设置操作失败休息时间（单位：毫秒）
     *
     * @param failedSleepTime 操作失败休息时间（单位：毫秒）
     */
    public TriangleArbitrage setFailedSleepTime(Long failedSleepTime) {
        this.failedSleepTime = failedSleepTime;
        return this;
    }

    /**
     * 表示市场行情有效时间，
     * 即在获取行情数据时，交易所必须在此时间之内返回行情数据，否则认为获取到的数据无效
     */
    private Long marketAvailableDuration;

    /**
     * 获取市场有效时间
     * <p>
     * 即在获取行情数据时，交易所必须在此时间之内返回行情数据，否则认为获取到的数据无效
     *
     * @return 市场有效时间
     */
    public Long getMarketAvailableDuration() {
        return marketAvailableDuration;
    }

    /**
     * 设置市场有效时间
     * <p>
     * 即在获取行情数据时，交易所必须在此时间之内返回行情数据，否则认为获取到的数据无效
     *
     * @param marketAvailableDuration 市场有效时间
     */
    public TriangleArbitrage setMarketAvailableDuration(Long marketAvailableDuration) {
        this.marketAvailableDuration = marketAvailableDuration;
        return this;
    }

    /**
     * 单笔订单最大交易量，以币种 C 为单位
     */
    private BigDecimal maxQuantityPerOrder;

    /**
     * 获取单笔订单最大交易量，以币种 C 为单位
     *
     * @return 单笔订单最大交易量，以币种 C 为单位
     */
    public BigDecimal getMaxQuantityPerOrder() {
        return maxQuantityPerOrder;
    }

    /**
     * 设置单笔订单最大交易量，以币种 C 为单位
     *
     * @param maxQuantityPerOrder 单笔订单最大交易量，以币种 C 为单位
     */
    public TriangleArbitrage setMaxQuantityPerOrder(BigDecimal maxQuantityPerOrder) {
        this.maxQuantityPerOrder = maxQuantityPerOrder;
        return this;
    }

    /**
     * 单笔订单最小量，以币种 C 为单位
     */
    private BigDecimal minQuantityPerOrder;

    /**
     * 获取单笔订单最小量，以币种 C 为单位
     *
     * @return 单笔订单最小量，以币种 C 为单位
     */
    public BigDecimal getMinQuantityPerOrder() {
        return minQuantityPerOrder;
    }

    /**
     * 获取单笔订单最小量，以币种 C 为单位
     *
     * @param minQuantityPerOrder 单笔订单最小量，以币种 C 为单位
     */
    public TriangleArbitrage setMinQuantityPerOrder(BigDecimal minQuantityPerOrder) {
        this.minQuantityPerOrder = minQuantityPerOrder;
        return this;
    }

    /**
     * 币种A最多冻结数量
     */
    private BigDecimal currAmaxFreezeQuantity;

    /**
     * 获取币种A最多冻结数量
     *
     * @return 币种A最多冻结数量
     */
    public BigDecimal getCurrAmaxFreezeQuantity() {
        return currAmaxFreezeQuantity;
    }

    /**
     * 设置币种A最多冻结数量
     *
     * @param currAmaxFreezeQuantity 币种A最多冻结数量
     */
    public TriangleArbitrage setCurrAmaxFreezeQuantity(BigDecimal currAmaxFreezeQuantity) {
        this.currAmaxFreezeQuantity = currAmaxFreezeQuantity;
        return this;
    }

    /**
     * 币种B最多冻结数量
     */
    private BigDecimal currBmaxFreezeQuantity;

    /**
     * 获取币种A最多冻结数量
     *
     * @return 币种B最多冻结数量
     */
    public BigDecimal getCurrBmaxFreezeQuantity() {
        return currBmaxFreezeQuantity;
    }

    /**
     * 设置币种B最多冻结数量
     *
     * @param currBmaxFreezeQuantity 币种B最多冻结数量
     */
    public TriangleArbitrage setCurrBmaxFreezeQuantity(BigDecimal currBmaxFreezeQuantity) {
        this.currBmaxFreezeQuantity = currBmaxFreezeQuantity;
        return this;
    }

    /**
     * 币种A最多冻结数量
     */
    private BigDecimal currCmaxFreezeQuantity;

    /**
     * 获取币种A最多冻结数量
     *
     * @return 币种A最多冻结数量
     */
    public BigDecimal getCurrCmaxFreezeQuantity() {
        return currCmaxFreezeQuantity;
    }

    /**
     * 设置币种A最多冻结数量
     *
     * @param currCmaxFreezeQuantity 币种A最多冻结数量
     */
    public TriangleArbitrage setCurrCmaxFreezeQuantity(BigDecimal currCmaxFreezeQuantity) {
        this.currCmaxFreezeQuantity = currCmaxFreezeQuantity;
        return this;
    }

    /**
     * 通用精度，即中间计算时保留的精度
     */
    private Integer commonScale;

    /**
     * 获取通用精度，即中间计算时保留的精度
     * @return 通用精度，即中间计算时保留的精度
     */
    public Integer getCommonScale() {
        return commonScale;
    }

    /**
     * 设置通用精度，即中间计算时保留的精度
     * @param commonScale 通用精度，即中间计算时保留的精度
     */
    public TriangleArbitrage setCommonScale(Integer commonScale) {
        this.commonScale = commonScale;
        return this;
    }

    /**
     * 同步挂单的频次，即执行多少轮就进行一次挂单的同步
     */
    private int syncOrderFrequency;

    /**
     * 获取同步挂单的频次，即执行多少轮就进行一次挂单的同步
     * @return 同步挂单的频次，即执行多少轮就进行一次挂单的同步
     */
    public int getSyncOrderFrequency() {
        return syncOrderFrequency;
    }

    /**
     * 设置同步挂单的频次，即执行多少轮就进行一次挂单的同步
     * @param syncOrderFrequency 同步挂单的频次，即执行多少轮就进行一次挂单的同步
     */
    public TriangleArbitrage setSyncOrderFrequency(int syncOrderFrequency) {
        this.syncOrderFrequency = syncOrderFrequency;
        return this;
    }

    /**
     * 同步余额的频次，一般在手动操作了需要用到此方法，平时只要执行了挂单或者同步挂单，此策略都会同步一次余额信息，所以此值可以设置大一些。
     */
    private int syncBalanceFrequency;

    /**
     * 获取同步余额的频次，一般在手动操作了需要用到此方法，平时只要执行了挂单或者同步挂单，此策略都会同步一次余额信息，所以此值可以设置大一些。
     * @return 获取同步余额的频次，一般在手动操作了需要用到此方法，平时只要执行了挂单或者同步挂单，此策略都会同步一次余额信息，所以此值可以设置大一些。
     */
    public int getSyncBalanceFrequency() {
        return syncBalanceFrequency;
    }

    /**
     * 设置获取同步余额的频次，一般在手动操作了需要用到此方法，平时只要执行了挂单或者同步挂单，此策略都会同步一次余额信息，所以此值可以设置大一些。
     * @param syncBalanceFrequency 获取同步余额的频次，一般在手动操作了需要用到此方法，平时只要执行了挂单或者同步挂单，此策略都会同步一次余额信息，所以此值可以设置大一些。
     */
    public TriangleArbitrage setSyncBalanceFrequency(int syncBalanceFrequency) {
        this.syncBalanceFrequency = syncBalanceFrequency;
        return this;
    }

    /*--------------------------------全局变量--------------------------------*/


    /**
     * 交易对列表
     */
    private List<CurrencyPair> currPairs;

    /**
     * 交易所实例
     */
    private List<Exchange> exchanges;

    /**
     * 余额信息
     */
    private Map<String, Account.Balance> balances;

    /**
     * 上一次的余额信息，主要用于和当前余额进行比较，若当前余额相对于上一次余额没有减少
     * ，证明挂单成交了，可以不用特意去同步挂单
     */
    private Map<String, Account.Balance> lastBalances;


    /**
     * 深度信息
     */
    private Map<CurrencyPair, Depth> depthes;

    /**
     * API调用失败休息时间
     */
    private Long failedSleepSeconds;

    /**
     * 币种A，只做计价币
     */
    private String currA;

    /**
     * 币种B，可做基础币和计价币
     */
    private String currB;

    /**
     * 币种C，只能做基础币
     */
    private String currC;

    /**
     * 最大的价格精度
     */
    private Integer maxPriceScale;

    /**
     * 最大的量精度
     */
    private Integer maxQuantityScale;

    /**
     * 数据库访问对象
     */
    private CommonDao dao;

    /**
     * 进行中的订单
     */
    private Map<CurrencyPair, List<TriangleArbitrageOrder>> liveOrders;

    /**
     * 计划中的订单
     */
    private Map<CurrencyPair, List<TriangleArbitrageOrder>> planOrders;

    /**
     * 订单排序规则，买单先于卖单，价格高的在前，价格低的在后
     */
    private Comparator<TriangleArbitrageOrder> orderingRule;

    /**
     * 当前交易所的下标
     */
    private int currentExchangeIndex = 0;

    private synchronized Exchange getExchange(){
        if(currentExchangeIndex == exchanges.size()) {
            currentExchangeIndex = 0;
        }
        return exchanges.get(currentExchangeIndex++);
    }

    /*--------------------------------私有方法--------------------------------*/

    /**
     * 初始化程序
     *
     * @return 初始化结果
     */
    private Boolean init() {
        Boolean result = true;
        logger.info("开始初始化策略。");

        this.currPairs = new ArrayList<>();
        this.currPairs.add(currPairBA);
        this.currPairs.add(currPairCA);
        this.currPairs.add(currPairCB);

        this.failedSleepSeconds = this.failedSleepTime / 1000;

        this.maxPriceScale = Integer.max(Integer.max(currPairBA.getPriceScale(), currPairCA.getPriceScale()), currPairCB.getPriceScale());

        this.maxQuantityScale = Integer.max(Integer.max(currPairBA.getQuantityScale(), currPairCA.getQuantityScale()), currPairCB.getQuantityScale());

        this.exchanges = new ArrayList<>();
        for(int i=0; i<3; i++){
            Exchange exchange = EndExchangeFactory.newInstance(exchangeName);
            if (null == exchange) {
                result = false;
            }
            exchanges.add(exchange);
        }

        this.liveOrders = new HashMap<>();
        this.liveOrders.put(currPairBA, new ArrayList<>());
        this.liveOrders.put(currPairCA, new ArrayList<>());
        this.liveOrders.put(currPairCB, new ArrayList<>());

        this.planOrders = new HashMap<>();
        this.planOrders.put(currPairBA, new ArrayList<>());
        this.planOrders.put(currPairCA, new ArrayList<>());
        this.planOrders.put(currPairCB, new ArrayList<>());

        orderingRule = (a, b)-> a.getOrderSide().equals(b.getOrderSide()) ?
                b.getOrderPrice().compareTo(a.getOrderPrice()) :
                    a.getOrderSide().compareTo(b.getOrderSide());

        this.dao = new CommonDao();

        syncOrdersFromDB();

        syncBalances();

        syncOrders();

        logger.info("策略初始化结束。");
        return result;
    }

    /**
     * 校验设置的参数
     *
     * @return 参数校验结果
     */
    private Boolean validateParameters() {

        Boolean result = true;
        logger.info("开始校验参数。");
        if (null == this.currPairBA) {
            logger.error("未设置BA交易对。");
            result = false;
        } else {
            logger.info("BA交易对: {}", this.currPairBA.getCurrencyPair());
        }

        if (null == this.currPairCB) {
            logger.error("未设置CB交易对。");
            result = false;
        } else {
            logger.info("CB交易对: {}", this.currPairCB.getCurrencyPair());
        }

        if (null == this.currPairCA) {
            logger.error("未设置CA交易对");
            result = false;
        } else {
            logger.info("CA交易对：{}。", this.currPairCA.getCurrencyPair());
        }

        if (null != currPairBA && null != currPairCA && null != currPairCB) {
            String[] currPairBAArr = currPairBA.getCurrencyPair().split("_");
            String[] currPairCAArr = currPairCA.getCurrencyPair().split("_");
            String[] currPairCBArr = currPairCB.getCurrencyPair().split("_");
            if (!currPairBAArr[0].equals(currPairCBArr[1])
                    || !currPairBAArr[1].equals(currPairCAArr[1])
                    || !currPairCBArr[0].equals(currPairCAArr[0])) {
                logger.error("设置的币种交易对{},{},{}无法构成三角套利。", currPairBA.getCurrencyPair(), currPairCA.getCurrencyPair(), currPairCB.getCurrencyPair());
                result = false;
            } else {
                this.currA = currPairBAArr[1];
                this.currB = currPairBAArr[0];
                this.currC = currPairCBArr[0];
            }
        }

        if (null == this.arbitrageCurr && this.currPairCA != null) {
            this.arbitrageCurr = this.currPairBA.getCurrencyPair().split("_")[1];
            logger.warn("未设置套利币种，默认以计价币({})作为套利币种。", this.arbitrageCurr);
        } else {
            logger.info("套利币种：{}", this.arbitrageCurr);
        }

        if (null == this.failedSleepTime) {
            logger.warn("未设置API操作失败休息时间，设置默认值：{}ms", 5000);
            this.failedSleepTime = 5000L;
        } else {
            logger.info("API操作失败休息时间：{}ms。", this.failedSleepTime);
        }

        if (null == this.marketAvailableDuration) {
            logger.warn("未设置市场有效时间，设置默认值：{}ms", 1000);
            this.marketAvailableDuration = 1000L;
        } else {
            logger.info("市场有效时间：{}ms", this.marketAvailableDuration);
        }

        if (null == this.exchangeName) {
            logger.error("未设置交易所。");
            result = false;
        } else {
            logger.info("交易所：{}", this.exchangeName);
        }

        if(null == this.commonScale){
            logger.warn("未设置通用精度，通用精度设置为默认值 25");
            this.commonScale = 25;
        }else {
            logger.info("通用精度：{}", commonScale);
        }

        if(null == this.maxQuantityPerOrder){
            logger.error("未设置单笔操作{}最大量。", currC);
            result = false;
        }else {
            logger.info("单笔操作最大量：{}{}", this.maxQuantityPerOrder,this.currC);
        }

        if(null == this.minQuantityPerOrder){
            logger.error("未设置单笔操作{}最小量。", currC);
            result = false;
        }else {
            logger.info("单笔操作最小量：{}{}", this.minQuantityPerOrder, this.currC);
        }

        if(null != this.maxQuantityPerOrder && null != this.minQuantityPerOrder){
            if(this.maxQuantityPerOrder.compareTo(this.minQuantityPerOrder) <= 0){
                logger.error("单笔操作最大量{}{}不能少于单笔操作最小量{}{}", this.maxQuantityPerOrder, this.currC, this.minQuantityPerOrder, currC);
                result = false;
            }
        }

        if(0==this.syncOrderFrequency){
            logger.warn("未设置订单同步频次，设置默认值为 200");
            this.syncOrderFrequency = 200;
        }else {
            logger.info("订单同步频次：{}", this.syncOrderFrequency);
        }

        if(0==this.syncBalanceFrequency){
            logger.warn("未设置余额同步频次，设置默认值为 2000");
            this.syncBalanceFrequency = 2000;
        }else {
            logger.info("余额同步频次：{}", this.syncBalanceFrequency);
        }

        logger.info("参数校验结束。");
        return result;
    }

    /**
     * 同步余额信息，直到同步成功此函数才结束
     */
    private void syncBalances() {
        Account account = null;
        while (null == account) {
            if (null == (account = getExchange().getAccount())) {
                logger.error("同步余额信息失败，{}秒后重试。", this.failedSleepSeconds);
                TimeUtil.delay(failedSleepTime);
            }
        }
        balances = account.getBalances();
    }

    /**
     * 同步深度信息，直到同步到有效深度信息才结束此方法。
     */
    private void syncDepth() {
        this.depthes = new ConcurrentHashMap<>();
        while (true) {
            Long begin = System.currentTimeMillis();
            currPairs.parallelStream().forEach(e -> {
                Depth depth = getExchange().getDepth(e.getCurrencyPair());
                if (null != depth) {
                    depthes.put(e, depth);
                }
            });
            Long end = System.currentTimeMillis();

            // 并非三个币种对都获取到了深度信息
            if (depthes.size() != 3) {
                logger.error("未能成功同步深度信息，{}秒后重试。", this.failedSleepSeconds);
                this.depthes.clear();
                TimeUtil.delay(failedSleepTime);
                continue;
            }

            // 未在有效时间内获取到三个交易币种对的深度信息
            if (end - begin > this.marketAvailableDuration) {
                logger.warn("未能在有效时间内同步深度信息，市场有效时间{}ms，实际花费时间{}ms，立即重试。", this.marketAvailableDuration, end - begin);
                this.depthes.clear();
                continue;
            }
            break;
        }
    }

    /**
     * 从数据库中同步订单信息
     */
    private void syncOrdersFromDB(){
        String hql = "from TriangleArbitrageOrder where (orderStatus = 'NEW' or orderStatus='PLAN') and exchangeName =:exchangeName ";
        Map<String, Object> params = new HashMap<>();
        params.put("exchangeName", this.exchangeName);
        List<TriangleArbitrageOrder> unfilledOrders = dao.findByHql(hql, params, TriangleArbitrageOrder.class);
        unfilledOrders.forEach(e ->{
            String quoteCurr = e.getQuoteCurrency();
            String baseCurr = e.getBaseCurrency();
            if(quoteCurr.equals(currA) && baseCurr.equals(currB)){
                if(OrderStatus.NEW.equals(e.getOrderStatus())){
                    liveOrders.get(currPairBA).add(e);
                }else {
                    planOrders.get(currPairBA).add(e);
                }
            }else if(quoteCurr.equals(currA) && baseCurr.equals(currC)){
                if(OrderStatus.NEW.equals(e.getOrderStatus())){
                    liveOrders.get(currPairCA).add(e);
                }else {
                    planOrders.get(currPairCA).add(e);
                }
            }else if(quoteCurr.equals(currB) && baseCurr.equals(currC)){
                if(OrderStatus.NEW.equals(e.getOrderStatus())){
                    liveOrders.get(currPairCB).add(e);
                }else {
                    planOrders.get(currPairCB).add(e);
                }
            }
        });

        this.liveOrders.forEach((k, v) -> v.sort(orderingRule));
        this.planOrders.forEach((k, v) -> v.sort(orderingRule));

    }

    /**
     * 同步订单信息，同步失败，暂停一会继续同步
     */
    private void syncOrders(){

        liveOrders.forEach((k, v)->{

            for (int i=0; i<v.size(); i++){

                TriangleArbitrageOrder currOrder = v.get(i);

                if(!OrderSide.BUY.equals(currOrder.getOrderSide())){
                    break;
                }

                Order order = getExchange().getOrder(k.getCurrencyPair(), currOrder.getOrderId());
                if(null == order){
                    logger.error("同步订单（orderSide={}, currencyPair={}, orderId={}）失败，{}秒后重试。", currOrder.getOrderSide(),k.getCurrencyPair(), currOrder.getOrderId(), this.failedSleepSeconds);
                    TimeUtil.delay(failedSleepTime);
                    i--;
                }else if(OrderStatus.FILLED.equals(order.getStatus()) || OrderStatus.CANCELED.equals(order.getStatus())){
                    currOrder.setOrderStatus(order.getStatus());
                    currOrder.setAvgPrice(order.getTradeMoney().divide(order.getTradeQuantity(), k.getPriceScale(), RoundingMode.UP));
                    currOrder.setModifyTimestamp(System.currentTimeMillis());
                    dao.saveOrUpdate(currOrder);
                    v.remove(i);
                }else {
                    break;
                }
            }

            for (int i=v.size()-1; i>-1; i--){
                TriangleArbitrageOrder currOrder = v.get(i);
                if(!OrderSide.SELL.equals(currOrder.getOrderSide())){
                    break;
                }

                Order order = getExchange().getOrder(k.getCurrencyPair(), currOrder.getOrderId());
                if(null == order){
                    logger.error("同步订单（orderSide={}, currencyPair={}, orderId={}）失败，{}秒后重试。", currOrder.getOrderSide(),k.getCurrencyPair(), currOrder.getOrderId());
                    TimeUtil.delay(failedSleepSeconds);
                    i++;
                    continue;
                }

                if(OrderStatus.FILLED.equals(order.getStatus()) || OrderStatus.CANCELED.equals(order.getStatus())){
                    currOrder.setOrderStatus(order.getStatus());
                    currOrder.setAvgPrice(order.getTradeMoney().divide(order.getTradeQuantity(), k.getPriceScale(), RoundingMode.DOWN));
                    currOrder.setModifyTimestamp(System.currentTimeMillis());
                    dao.saveOrUpdate(currOrder);
                    v.remove(i);
                }else {
                    break;
                }
            }
            Collections.sort(v, orderingRule);
        });

    }

    /**
     * 计算挂单信息
     */
    private Boolean calculateOrders() {

        Depth.PriceQuotation ab = depthes.get(currPairBA).getAsks().get(0);
        Depth.PriceQuotation ba = depthes.get(currPairBA).getBids().get(0);
        Depth.PriceQuotation bc = depthes.get(currPairCB).getAsks().get(0);
        Depth.PriceQuotation cb = depthes.get(currPairCB).getBids().get(0);
        Depth.PriceQuotation ca = depthes.get(currPairCA).getBids().get(0);
        Depth.PriceQuotation ac = depthes.get(currPairCA).getAsks().get(0);

        // 不带手续费费率的汇率，例如 Pab表示不带手续费1个A能换Pab个B
        BigDecimal Pab = BigDecimal.ONE.divide(ab.getPrice(), maxPriceScale + 5, RoundingMode.DOWN);
        BigDecimal Pba = ba.getPrice();
        BigDecimal Pbc = BigDecimal.ONE.divide(bc.getPrice(), maxPriceScale + 5, RoundingMode.DOWN);
        BigDecimal Pcb = cb.getPrice();
        BigDecimal Pca = ca.getPrice();
        BigDecimal Pac = BigDecimal.ONE.divide(ac.getPrice(), maxPriceScale + 5, RoundingMode.DOWN);


        // 带上手续费费率之后的汇率，例如 Pabf表示带上手续费之后1个A能换Pabf个B
        BigDecimal Pabf = Pab.multiply(BigDecimal.ONE.subtract(currPairBA.getBuyFeeRate()));
        BigDecimal Pbaf = Pba.multiply(BigDecimal.ONE.subtract(currPairBA.getSellFeeRate()));
        BigDecimal Pbcf = Pbc.multiply(BigDecimal.ONE.subtract(currPairCB.getBuyFeeRate()));
        BigDecimal Pcbf = Pcb.multiply(BigDecimal.ONE.subtract(currPairCB.getSellFeeRate()));
        BigDecimal Pcaf = Pca.multiply(BigDecimal.ONE.subtract(currPairCA.getSellFeeRate()));
        BigDecimal Pacf = Pac.multiply(BigDecimal.ONE.subtract(currPairCA.getBuyFeeRate()));

        BigDecimal Qab = ab.getQuantity();//表示一个兑换最多可以得到多少币种B
        BigDecimal Qba = ba.getPrice().multiply(ba.getQuantity());//表示一个兑换最多可以得到多少币种A
        BigDecimal Qbc = bc.getQuantity();
        BigDecimal Qcb = cb.getPrice().multiply(cb.getQuantity());
        BigDecimal Qca = ca.getPrice().multiply(ca.getQuantity());
        BigDecimal Qac = ac.getQuantity();

        // 下单的量
        BigDecimal S_Qab = new BigDecimal("0");
        BigDecimal S_Qba = new BigDecimal("0");
        BigDecimal S_Qbc = new BigDecimal("0");
        BigDecimal S_Qcb = new BigDecimal("0");
        BigDecimal S_Qca = new BigDecimal("0");
        BigDecimal S_Qac = new BigDecimal("0");

        // 下单的价格
        BigDecimal P_ab=null;
        BigDecimal P_ba=null;
        BigDecimal P_bc=null;
        BigDecimal P_cb=null;
        BigDecimal P_ca=null;
        BigDecimal P_ac=null;

        // 逆时针套利系数
        BigDecimal antiClockwise = calculateArbitrageCoefficient(Pbaf, Pacf, Pcbf);

        // 顺时针套利系数
        BigDecimal clockwise = calculateArbitrageCoefficient(Pabf, Pbcf, Pcaf);

        Integer abIdx = 0;
        Integer baIdx = 0;
        Integer bcIdx = 0;
        Integer cbIdx = 0;
        Integer caIdx = 0;
        Integer acIdx = 0;

        logger.info("顺时针套利系数：{}, 逆时针套利系数：{}", clockwise.stripTrailingZeros().toPlainString(), antiClockwise.stripTrailingZeros().toPlainString());

        // 存在顺时针套利机会或者逆时针套利机会
        while (clockwise.compareTo(BigDecimal.ONE) > 0) {
            logger.info("顺时针套利系数：{}", clockwise);
            BigDecimal Pabf_mult_Pbcf = Pabf.multiply(Pbcf);
            //BigDecimal Pbcf_mult_Pcaf = Pbcf.multiply(Pcaf);
            //BigDecimal Pcaf_mult_Pabf = Pcaf.multiply(Pabf);
            BigDecimal Pabf_mult_Pbcf_mult_Pcaf = Pabf_mult_Pbcf.multiply(Pcaf);

            P_ab = ab.getPrice();
            P_bc = bc.getPrice();
            P_ca = ca.getPrice();

            // 套币种A
            if (arbitrageCurr.equals(currA)) {

                // 计算最小的量系数作为三者的量系数
                BigDecimal QabK = Qab.divide(Pabf, commonScale, RoundingMode.HALF_EVEN);
                BigDecimal QbcK = Qbc.divide(Pabf_mult_Pbcf, commonScale, RoundingMode.HALF_EVEN);
                BigDecimal QcaK = Qca.divide(Pabf_mult_Pbcf_mult_Pcaf, commonScale, RoundingMode.HALF_EVEN);
                BigDecimal QminK = BigDecimalUtil.min(QabK, QbcK, QcaK);

                BigDecimal QabDecrement = BigDecimal.ZERO;
                BigDecimal QbcDecrement = BigDecimal.ZERO;
                BigDecimal QcaDecrement = BigDecimal.ZERO;



                if (QminK.equals(QabK)) {

                    QabDecrement = Qab;
                    QbcDecrement = QminK.multiply(Pabf_mult_Pbcf);
                    QcaDecrement = QminK.multiply(Pabf_mult_Pbcf_mult_Pcaf).divide(Pca, commonScale, RoundingMode.DOWN);

                    ab = depthes.get(currPairBA).getAsks().get(++abIdx);
                    Pab = BigDecimal.ONE.divide(ab.getPrice(), commonScale, RoundingMode.DOWN);
                    Pabf = Pab.multiply(BigDecimal.ONE.subtract(currPairBA.getBuyFeeRate()));

                    Qab = ab.getQuantity();
                    Qbc = Qbc.subtract(QbcDecrement);
                    Qca = Qca.subtract(QminK.multiply(Pabf_mult_Pbcf_mult_Pcaf));
                } else if (QminK.equals(QbcK)) {
                    QabDecrement = QminK.multiply(Pabf);
                    QbcDecrement = Qbc;
                    QcaDecrement = QminK.multiply(Pabf_mult_Pbcf_mult_Pcaf).divide(Pca, commonScale, RoundingMode.DOWN);

                    bc = depthes.get(currPairCB).getAsks().get(++bcIdx);
                    Pbc = BigDecimal.ONE.divide(bc.getPrice(), commonScale, RoundingMode.DOWN);
                    Pbcf = Pbc.multiply(BigDecimal.ONE.subtract(currPairCB.getBuyFeeRate()));

                    Qab = Qab.subtract(QabDecrement);
                    Qbc = bc.getQuantity();
                    Qca = Qca.subtract(QminK.multiply(Pabf_mult_Pbcf_mult_Pcaf));
                } else if (QminK.equals(QcaK)) {
                    QabDecrement = QminK.multiply(Pabf);
                    QbcDecrement = QminK.multiply(Pabf_mult_Pbcf);
                    QcaDecrement = Qca.divide(Pca, commonScale, RoundingMode.DOWN);

                    ca = depthes.get(currPairCA).getBids().get(++caIdx);
                    Pca = ca.getPrice();
                    Pcaf = Pca.multiply(BigDecimal.ONE.subtract(currPairCA.getSellFeeRate()));

                    Qab = Qab.subtract(QabDecrement);
                    Qbc = Qbc.subtract(QbcDecrement);
                    Qca = ca.getPrice().multiply(ca.getQuantity());
                } else {
                    logger.error("最小量系数计算存在问题。");
                }

                // 调整订单的数量
                if(S_Qbc.add(QbcDecrement).compareTo(this.maxQuantityPerOrder) > 0){
                    BigDecimal k = this.maxQuantityPerOrder.subtract(S_Qbc).divide(QbcDecrement, this.commonScale, RoundingMode.DOWN);
                    S_Qbc = this.maxQuantityPerOrder;
                    S_Qab = S_Qab.add(QabDecrement.multiply(k));
                    S_Qca = S_Qca.add(QcaDecrement.multiply(k));
                    break;
                }

                S_Qab = S_Qab.add(QabDecrement);
                S_Qbc = S_Qbc.add(QbcDecrement);
                S_Qca = S_Qca.add(QcaDecrement);

            } else {
                logger.error("顺时针套利币种{}的代码待完善。", arbitrageCurr);
                break;
            }

            clockwise = calculateArbitrageCoefficient(Pabf, Pbcf, Pcaf);
        }

        if(BigDecimal.ZERO.compareTo(S_Qab) < 0){

            // 少于每单最小量，不挂单
            if(S_Qbc.compareTo(this.minQuantityPerOrder) < 0){
                logger.debug("单笔订单{}挂单数量太少，不挂单，【最少 {} > 当前 {}】", currC, this.minQuantityPerOrder, S_Qbc);
                return false;
            }

            BigDecimal _p_ab = P_ab.stripTrailingZeros();
            BigDecimal _p_bc = P_bc.stripTrailingZeros();
            BigDecimal _p_ca = P_ca.stripTrailingZeros();

            BigDecimal _q_ab = S_Qab.divide(BigDecimal.ONE, currPairBA.getQuantityScale(), RoundingMode.DOWN).stripTrailingZeros();
            BigDecimal _q_bc = S_Qbc.divide(BigDecimal.ONE, currPairCB.getQuantityScale(), RoundingMode.DOWN).stripTrailingZeros();
            BigDecimal _q_ca = S_Qca.divide(BigDecimal.ONE, currPairCA.getQuantityScale(), RoundingMode.DOWN).stripTrailingZeros();

            logger.debug("{} -> {} 买单（{}价格买入{} {}） 得到{} {}, 消耗{} {}", currA, currB, _p_ab.toPlainString(), _q_ab.toPlainString(), currB, _q_ab.multiply(BigDecimal.ONE.subtract(currPairBA.getBuyFeeRate())).stripTrailingZeros().toPlainString(),currB, _q_ab.multiply(_p_ab).stripTrailingZeros().toPlainString(), currA);
            logger.debug("{} -> {} 买单（{}价格买入{} {}）得到{} {}, 消耗{} {}", currB, currC, _p_bc.toPlainString(), _q_bc.toPlainString(), currC, _q_bc.multiply(BigDecimal.ONE.subtract(currPairCB.getBuyFeeRate())).stripTrailingZeros().toPlainString(), currC, _q_bc.multiply(_p_bc).stripTrailingZeros().toPlainString(), currB);
            logger.debug("{} -> {} 卖单（{}价格卖出{} {}） 得到{} {}, 消耗{} {}", currC, currA, _p_ca.toPlainString(), _q_ca.toPlainString(), currC, _q_ca.multiply(_p_ca).multiply(BigDecimal.ONE.subtract(currPairCA.getSellFeeRate())).stripTrailingZeros().toPlainString(), currA, _q_ca.stripTrailingZeros().toPlainString(), currC);

            // 检查余额是否充足
            BigDecimal currAbalFree = balances.get(currA).getFree();
            if(currAbalFree.compareTo(_q_ab.multiply(_p_ab)) < 0){
                logger.error("{}余额不足，需要{}，可用{}", currA, _q_ab.multiply(_p_ab), currAbalFree);
                return false;
            }

            BigDecimal currBbalFree = balances.get(currB).getFree();
            if(currBbalFree.compareTo(_q_bc.multiply(_p_bc)) < 0){
                logger.error("{}余额不足，需要{}，可用{}", currB, _q_bc.multiply(_p_bc), currBbalFree);
                return false;
            }

            BigDecimal currCbalFree = balances.get(currC).getFree();
            if(currCbalFree.compareTo(_q_ca) < 0){
                logger.error("{}余额不足，需要{}，可用{}", currC, _q_ca, currCbalFree);
                return false;
            }

            String groupId = UUID.randomUUID().toString().replace("-", "");
            List<TriangleArbitrageOrder> orders = new ArrayList<>();
            orders.add(buildTriangleArbitrageOrder(OrderSide.BUY, currPairBA, _p_ab, _q_ab, groupId));
            orders.add(buildTriangleArbitrageOrder(OrderSide.SELL, currPairCA, _p_ca, _q_ca, groupId));
            orders.add(buildTriangleArbitrageOrder(OrderSide.BUY, currPairCB, _p_bc, _q_bc, groupId));
            placeOrders(orders);
            return true;
        }


        while (antiClockwise.compareTo(BigDecimal.ONE) > 0) {
            logger.info("逆时针套利系数：{}", antiClockwise);
            //BigDecimal Pbaf_mult_Pacf = Pbaf.multiply(Pacf);
            BigDecimal Pacf_mult_Pcbf = Pacf.multiply(Pcbf);
            //BigDecimal Pcbf_mult_Pbaf = Pcbf.multiply(Pbaf);
            BigDecimal Pbaf_mult_Pacf_mult_Pcbf = Pbaf.multiply(Pacf).multiply(Pcbf);

            P_ba = ba.getPrice();
            P_ac = ac.getPrice();
            P_cb = cb.getPrice();

            //套利币种A
            if(arbitrageCurr.equals(currA)){

                // 计算最小的量系数作为三者的量系数
                BigDecimal QbaK = Qba.divide(Pbaf_mult_Pacf_mult_Pcbf, commonScale, RoundingMode.HALF_EVEN);
                BigDecimal QacK = Qac.divide(Pacf, commonScale, RoundingMode.HALF_EVEN);
                BigDecimal QcbK = Qcb.divide(Pacf_mult_Pcbf, commonScale, RoundingMode.HALF_EVEN);
                BigDecimal QminK = BigDecimalUtil.min(QbaK, QacK, QcbK);

                BigDecimal QbaDecrement = BigDecimal.ZERO;
                BigDecimal QacDecrement = BigDecimal.ZERO;
                BigDecimal QcbDecrement = BigDecimal.ZERO;

                if (QbaK.equals(QminK)){
                    QbaDecrement = Qba.divide(ba.getPrice(), commonScale, RoundingMode.HALF_EVEN);
                    QacDecrement = QminK.multiply(Pacf);
                    QcbDecrement = QminK.multiply(Pacf_mult_Pcbf).divide(cb.getPrice(), commonScale, RoundingMode.HALF_EVEN);

                    ba = depthes.get(currPairBA).getBids().get(++baIdx);
                    Pba = ba.getPrice();
                    Pbaf = Pba.multiply(BigDecimal.ONE.subtract(currPairBA.getSellFeeRate()));

                    Qba = ba.getPrice().multiply(ba.getQuantity());
                    Qac = Qac.subtract(QacDecrement);
                    Qcb = Qcb.subtract(QminK.multiply(Pacf_mult_Pcbf));

                }else if(QacK.equals(QminK)){
                    QbaDecrement = QminK.multiply(Pbaf_mult_Pacf_mult_Pcbf).divide(ba.getPrice(), commonScale, RoundingMode.HALF_EVEN);
                    QacDecrement = Qac;
                    QcbDecrement = QminK.multiply(Pacf_mult_Pcbf).divide(cb.getPrice(), commonScale, RoundingMode.HALF_EVEN);

                    ac = depthes.get(currPairCA).getAsks().get(++acIdx);
                    Pac = BigDecimal.ONE.divide(ac.getPrice(), maxPriceScale + 5, RoundingMode.DOWN);
                    Pacf = Pac.multiply(BigDecimal.ONE.subtract(currPairCA.getBuyFeeRate()));

                    Qba = Qba.subtract(QminK.multiply(Pbaf_mult_Pacf_mult_Pcbf));
                    Qac = ac.getQuantity();
                    Qcb = Qcb.subtract(QminK.multiply(Pacf_mult_Pcbf));
                }

                else if(QcbK.equals(QminK)){

                    QbaDecrement = QminK.multiply(Pbaf_mult_Pacf_mult_Pcbf).divide(ba.getPrice(), commonScale, RoundingMode.HALF_EVEN);
                    QacDecrement = QminK.multiply(Pacf);
                    QcbDecrement = Qcb.divide(cb.getPrice(), commonScale, RoundingMode.HALF_EVEN);

                    cb = depthes.get(currPairCB).getBids().get(++cbIdx);
                    Pcb = cb.getPrice();
                    Pcbf = Pcb.multiply(BigDecimal.ONE.subtract(currPairCB.getSellFeeRate()));

                    Qba = Qba.subtract(QminK.multiply(Pbaf_mult_Pacf_mult_Pcbf));
                    Qac = Qac.subtract(QacDecrement);
                    Qcb = cb.getPrice().multiply(cb.getQuantity());
                } else{
                    logger.error("计算量系数是出错。");
                }

                //多于每单最大量，将数量调小
                if(S_Qcb.add(QcbDecrement).compareTo(this.maxQuantityPerOrder)>0){

                    BigDecimal k = this.maxQuantityPerOrder.subtract(S_Qcb).divide(QcbDecrement, this.commonScale, RoundingMode.DOWN);
                    S_Qcb = this.maxQuantityPerOrder;
                    S_Qac = S_Qac.add(QacDecrement.multiply(k));
                    S_Qba = S_Qba.add(QbaDecrement.multiply(k));
                    break;
                }

                S_Qba = S_Qba.add(QbaDecrement);
                S_Qac = S_Qac.add(QacDecrement);
                S_Qcb = S_Qcb.add(QcbDecrement);

            }else {
                logger.error("逆时针套利币种{}的代码待完善。", arbitrageCurr);
                break;
            }

            antiClockwise = calculateArbitrageCoefficient(Pbaf, Pacf, Pcbf);
        }

        if(BigDecimal.ZERO.compareTo(S_Qba) < 0){

            // 少于每单最小量，不挂单
            if(S_Qcb.compareTo(this.minQuantityPerOrder) < 0){
                logger.debug("单笔订单{}挂单数量太少，不挂单，【最少 {} > 当前 {}】", currC, this.minQuantityPerOrder, S_Qcb);
                return false;
            }

            BigDecimal _p_ba = P_ba.stripTrailingZeros();
            BigDecimal _p_ac = P_ac.stripTrailingZeros();
            BigDecimal _p_cb = P_cb.stripTrailingZeros();

            BigDecimal _q_ba = S_Qba.divide(BigDecimal.ONE, currPairBA.getQuantityScale(), RoundingMode.DOWN).stripTrailingZeros();
            BigDecimal _q_ac = S_Qac.divide(BigDecimal.ONE, currPairCA.getQuantityScale(), RoundingMode.DOWN).stripTrailingZeros();
            BigDecimal _q_cb = S_Qcb.divide(BigDecimal.ONE, currPairCB.getQuantityScale(), RoundingMode.DOWN).stripTrailingZeros();

            logger.debug("{} -> {} 卖单（{}价格卖出{} {}） 得到{} {}, 消耗{} {}", currB, currA, _p_ba.toPlainString(), _q_ba.toPlainString(), currB, _q_ba.multiply(_p_ba).multiply(BigDecimal.ONE.subtract(currPairBA.getSellFeeRate())).stripTrailingZeros().toPlainString(),currA, _q_ba.stripTrailingZeros().toPlainString(), currB);
            logger.debug("{} -> {} 买单（{}价格买入{} {}）得到{} {}, 消耗{} {}", currA, currC, _p_ac.toPlainString(), _q_ac.toPlainString(), currC, _q_ac.multiply(BigDecimal.ONE.subtract(currPairCA.getBuyFeeRate())).stripTrailingZeros().toPlainString(), currC, _q_ac.multiply(_p_ac).stripTrailingZeros().toPlainString(), currA);
            logger.debug("{} -> {} 卖单（{}价格卖出{} {}） 得到{} {}, 消耗{} {}", currC, currB, _p_cb.toPlainString(), _q_cb.toPlainString(), currC, _p_cb.multiply(_q_cb).multiply(BigDecimal.ONE.subtract(currPairCB.getSellFeeRate())).stripTrailingZeros().toPlainString(), currB, _q_cb.stripTrailingZeros().toPlainString(), currC);


            // 检查余额是否充足
            BigDecimal currAbalFree = balances.get(currA).getFree();
            if(currAbalFree.compareTo(_q_ac.multiply(_p_ac)) < 0){
                logger.error("{}余额不足，需要{}，可用{}", currA, _q_ac.multiply(_p_ac), currAbalFree);
                return false;
            }

            BigDecimal currBbalFree = balances.get(currB).getFree();
            if(currBbalFree.compareTo(_q_ba) < 0){
                logger.error("{}余额不足，需要{}，可用{}", currB, _q_ba, currBbalFree);
                return false;
            }

            BigDecimal currCbalFree = balances.get(currC).getFree();
            if(currCbalFree.compareTo(_q_cb) < 0){
                logger.error("{}余额不足，需要{}，可用{}", currC, _q_cb, currCbalFree);
                return false;
            }

            String groupId = UUID.randomUUID().toString().replace("-", "");
            List<TriangleArbitrageOrder> orders = new ArrayList<>();
            orders.add(buildTriangleArbitrageOrder(OrderSide.SELL, currPairBA, _p_ba, _q_ba, groupId));
            orders.add(buildTriangleArbitrageOrder(OrderSide.BUY, currPairCA, _p_ac, _q_ac, groupId));
            orders.add(buildTriangleArbitrageOrder(OrderSide.SELL, currPairCB, _p_cb, _q_cb, groupId));
            placeOrders(orders);
            return true;
        }
        return false;
    }

    /**
     * 构建一个 TriangbleArbitrateOrder
     * @param orderSide 订单方向
     * @param currencyPair 币种对
     * @param price 价格
     * @param quantity 量
     * @param groupId 对冲ID
     * @return 一个 TriangbleArbitrateOrder
     */
    private TriangleArbitrageOrder buildTriangleArbitrageOrder(String orderSide, CurrencyPair currencyPair, BigDecimal price, BigDecimal quantity, String groupId){
        TriangleArbitrageOrder order = new TriangleArbitrageOrder();
        order.setCreateTimestamp(System.currentTimeMillis());
        order.setBaseCurrency(currencyPair.getBaseCurrency());
        order.setQuoteCurrency(currencyPair.getQuoteCurrency());
        order.setExchangeName(exchangeName);
        order.setFeeRate(OrderSide.BUY.equals(orderSide) ? currencyPair.getBuyFeeRate() : currencyPair.getSellFeeRate());
        order.setGroupId(groupId);
        order.setOrderPrice(price);
        order.setOrderQuantity(quantity);
        order.setOrderSide(orderSide);
        return order;
    }

    /**
     * 并行下所有订单，并保存到数据库中
     * @param orders 所有订单
     */
    private void placeOrders(List<TriangleArbitrageOrder> orders){
        orders.parallelStream().forEach(this::placeOrder);
        Long succNumber = orders.stream().filter(e -> OrderStatus.NEW.equals(e.getOrderStatus())).count();

        // 均挂单失败
        if(succNumber == 0){
            return;
        }

        //根据余额调整挂单状态
        lastBalances = balances;
        syncBalances();
        logger.debug("挂单前余额：{}:{}, {}:{}, {}:{}", currA, lastBalances.get(currA), currB, lastBalances.get(currB), currC, lastBalances.get(currC));
        logger.debug("最新余额：{}:{}, {}:{}, {}:{}", currA, balances.get(currA), currB, balances.get(currB), currC, balances.get(currC));

        // 若三笔订单均挂单成功，且三个币种的可用余额都没有减少，说明三个订单都已经完成了
        if(balances.get(currA).getFree().compareTo(lastBalances.get(currA).getFree()) >= 0
                && balances.get(currB).getFree().compareTo(lastBalances.get(currB).getFree()) >= 0
                && balances.get(currC).getFree().compareTo(lastBalances.get(currC).getFree()) >= 0){
            orders.parallelStream()
                    .filter(e -> OrderStatus.NEW.equals(e.getOrderStatus()))
                    .forEach(e -> e.setOrderStatus(OrderStatus.FILLED));
        }else {
            liveOrders.forEach((k, v)-> {
                orders.stream().filter(e -> OrderStatus.NEW.equals(e.getOrderStatus())).forEach(e -> {
                    if (k.getCurrencyPair().startsWith(e.getBaseCurrency()) && k.getCurrencyPair().endsWith(e.getQuoteCurrency())) {
                        v.add(e);
                    }
                });
                v.sort(orderingRule);
            });

            planOrders.forEach((k, v)-> {
                orders.stream().filter(e -> "PLAN".equals(e.getOrderStatus())).forEach(e -> {
                    if (k.getCurrencyPair().startsWith(e.getBaseCurrency()) && k.getCurrencyPair().endsWith(e.getQuoteCurrency())) {
                        v.add(e);
                    }
                });
                v.sort(orderingRule);
            });
        }
        logger.debug("进行中订单：{}", liveOrders);
        logger.debug("计划订单：{}", planOrders);
        orders.parallelStream().forEach(dao::saveOrUpdate);
    }

    /**
     * 下计划单
     */
    private void placePlanOrders(){
        this.planOrders.forEach((k, v)->{

            boolean isPlacePlanOrder = false;

            if(v.size() == 0){
                return;//这里的return是跳过本轮循环
            }

            TriangleArbitrageOrder currOrder = v.get(0);
            if(OrderSide.BUY.equals(currOrder.getOrderSide())){
                // 买入价高于卖一价
                if(currOrder.getOrderPrice().compareTo(depthes.get(k).getAsks().get(0).getPrice()) > 0
                        //且计价币的余额充足
                        && balances.get(k.getQuoteCurrency()).getFree().compareTo(currOrder.getOrderPrice().multiply(currOrder.getOrderQuantity()))>0){
                    placeOrder(currOrder);
                    if(!currOrder.getOrderStatus().equals("PLAN")){
                        dao.saveOrUpdate(currOrder);
                        liveOrders.get(k).add(currOrder);
                        v.remove(currOrder);
                        isPlacePlanOrder = true;
                    }
                }
            }

            if(v.size() == 0){
                return;//这里的return是跳过本轮循环
            }

            currOrder = v.get(v.size()-1);
            if(OrderSide.SELL.equals(currOrder.getOrderSide())){
                // 卖出价小于买一价
                if(currOrder.getOrderPrice().compareTo(depthes.get(k).getBids().get(0).getPrice()) < 0
                        //且卖出的基础币数量充足
                        && balances.get(k.getBaseCurrency()).getFree().compareTo(currOrder.getOrderQuantity()) > 0){
                    placeOrder(currOrder);
                    if(!currOrder.getOrderStatus().equals("PLAN")){
                        isPlacePlanOrder = true;
                        dao.saveOrUpdate(currOrder);
                        liveOrders.get(k).add(currOrder);
                        v.remove(currOrder);
                    }
                }
            }

            if(isPlacePlanOrder){
                syncBalances();
                v.sort(orderingRule);
                liveOrders.get(k).sort(orderingRule);
            }

        });
    }


    /**
     * 下一笔订单
     * @param order 订单对象
     */
    private void placeOrder(TriangleArbitrageOrder order){
        Order od = getExchange().order(order.getOrderSide(), order.getBaseCurrency() + "_" + order.getQuoteCurrency(), order.getOrderQuantity(), order.getOrderPrice());
        if(null != od){
            order.setOrderId(od.getOrderId());
            order.setOrderStatus(OrderStatus.NEW);
        }else {
            order.setOrderStatus("PLAN");
        }
        order.setModifyTimestamp(System.currentTimeMillis());
    }

    /**
     * 计算套利系数
     *
     * @param p1 带手续费费率的汇率1
     * @param p2 带手续费费率的汇率2
     * @param p3 带手续费费率的汇率3
     * @return 套利系数
     */
    private BigDecimal calculateArbitrageCoefficient(BigDecimal p1, BigDecimal p2, BigDecimal p3) {
        return p1.multiply(p2).multiply(p3);
    }

    @Override
    public void run(Map<String, Object> parameters) {

        // 参数校验
        if (!this.validateParameters()) {
            logger.error("参数校验失败， 此策略不再执行。");
            return;
        }

        // 初始化
        if (!this.init()) {
            logger.error("策略初始化失败，此策略不再执行。");
            return;
        }

        int cycleTimes = 1;

        while (true) {

            cycleTimes ++;
            if(cycleTimes>50000){
                cycleTimes = 1;
            }

            if(cycleTimes % syncBalanceFrequency == 0){
                syncBalances();
            }

            if(cycleTimes % syncOrderFrequency == 0){
                syncOrders();
            }

            syncDepth();
            //testDepthClockwise();
            //testDepthAntiClockwise();
            depthes.forEach((k, v) -> logger.debug(v));

            // 此轮没有下过单，则下计划单
            if(!calculateOrders()){
                placePlanOrders();
            }


        }


    }


    private void testDepthClockwise(){
        this.depthes = new HashMap<>();
        Depth VNB_ETH = new Depth();
        VNB_ETH.setTimestamp(System.currentTimeMillis());
        VNB_ETH.setExchange("vnbig.com");
        VNB_ETH.setCurrency("VNB_ETH");
        List<Depth.PriceQuotation> VNB_ETH_asks = new ArrayList<>();
        List<Depth.PriceQuotation> VNB_ETH_bids = new ArrayList<>();
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00001288"), new BigDecimal("12150.28766942")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00001289"), new BigDecimal("16200.38355923")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.0000129"), new BigDecimal("9112.71575206")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00001291"), new BigDecimal("6834.53681405")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00001292"), new BigDecimal("5125.90261054")));
        VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00001281"), new BigDecimal("100")));
        VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00001211"), new BigDecimal("200")));
        VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.000012"), new BigDecimal("213670.73132755")));
        VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.0000128"), new BigDecimal("580.50055131")));
        VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00001275"), new BigDecimal("10602.14042553")));
        VNB_ETH.setAsks(VNB_ETH_asks);
        VNB_ETH.setBids(VNB_ETH_bids);
        depthes.put(currPairCB, VNB_ETH);

        Depth VNB_BTC = new Depth();
        VNB_BTC.setTimestamp(System.currentTimeMillis());
        VNB_BTC.setExchange("vnbig.com");
        VNB_BTC.setCurrency("VNB_BTC");
        List<Depth.PriceQuotation> VNB_BTC_asks = new ArrayList<>();
        List<Depth.PriceQuotation> VNB_BTC_bids = new ArrayList<>();
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.0000008"), new BigDecimal("506.61049602")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000082"), new BigDecimal("589.06855695")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.0000009"), new BigDecimal("38000")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000092"), new BigDecimal("6020")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000083"), new BigDecimal("1352")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00000070"), new BigDecimal("225.75939474")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00000073"), new BigDecimal("35555")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00000071"), new BigDecimal("55555")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00000069"), new BigDecimal("35555")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00000067"), new BigDecimal("11112.57168634")));
        VNB_BTC.setAsks(VNB_ETH_asks);
        VNB_BTC.setBids(VNB_BTC_bids);
        depthes.put(currPairCA, VNB_BTC);

        Depth ETH_BTC = new Depth();
        ETH_BTC.setTimestamp(System.currentTimeMillis());
        ETH_BTC.setExchange("vnbig.com");
        ETH_BTC.setCurrency("ETH_BTC");
        List<Depth.PriceQuotation> ETH_BTC_asks = new ArrayList<>();
        List<Depth.PriceQuotation> ETH_BTC_bids = new ArrayList<>();
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.05569714"), new BigDecimal("1.842")));
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.05568341"), new BigDecimal("0.021758")));
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.0556819"), new BigDecimal("0.0229122")));
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.05539682"), new BigDecimal("0.135071")));
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.05520687"), new BigDecimal("0.0308537")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.05518663"), new BigDecimal("0.119662")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.05517"), new BigDecimal("0.0218382")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.05516673"), new BigDecimal("0.0663865")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.05515204"), new BigDecimal("0.0149116")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.05514015"), new BigDecimal("0.286059")));
        ETH_BTC.setAsks(ETH_BTC_asks);
        ETH_BTC.setBids(ETH_BTC_bids);
        depthes.put(currPairBA, ETH_BTC);

    }

    private void testDepthAntiClockwise(){this.depthes = new HashMap<>();
        Depth VNB_ETH = new Depth();
        VNB_ETH.setTimestamp(System.currentTimeMillis());
        VNB_ETH.setExchange("vnbig.com");
        VNB_ETH.setCurrency("VNB_ETH");
        List<Depth.PriceQuotation> VNB_ETH_asks = new ArrayList<>();
        List<Depth.PriceQuotation> VNB_ETH_bids = new ArrayList<>();
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000607"), new BigDecimal("999.99")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000606"), new BigDecimal("100999.98")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000605"), new BigDecimal("999.99")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000604"), new BigDecimal("31008.11")));
        VNB_ETH_asks.add(new Depth.PriceQuotation(new BigDecimal("0.00000601"), new BigDecimal("919.42")));
        VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00000599"), new BigDecimal("2263.56")));
        //VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00030153"), new BigDecimal("5170.89")));
        //VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00030126"), new BigDecimal("261.95")));
        //VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00030036"), new BigDecimal("3.7")));
        //VNB_ETH_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00030033"), new BigDecimal("432.51")));
        VNB_ETH.setAsks(VNB_ETH_asks);
        VNB_ETH.setBids(VNB_ETH_bids);
        depthes.put(currPairCB, VNB_ETH);

        Depth VNB_BTC = new Depth();
        VNB_BTC.setTimestamp(System.currentTimeMillis());
        VNB_BTC.setExchange("vnbig.com");
        VNB_BTC.setCurrency("VNB_BTC");
        List<Depth.PriceQuotation> VNB_BTC_asks = new ArrayList<>();
        List<Depth.PriceQuotation> VNB_BTC_bids = new ArrayList<>();
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.124802"), new BigDecimal("22.13")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.124803"), new BigDecimal("43563.87")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.124804"), new BigDecimal("4763.25")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.124806"), new BigDecimal("600")));
        VNB_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.124807"), new BigDecimal("650")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.0000002837"), new BigDecimal("64085.57")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.0000002818"), new BigDecimal("70000")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.0000002814"), new BigDecimal("100000")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.0000002701"), new BigDecimal("70000")));
        VNB_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.00000027"), new BigDecimal("57.65")));
        VNB_BTC.setAsks(VNB_BTC_asks);
        VNB_BTC.setBids(VNB_BTC_bids);
        depthes.put(currPairCA, VNB_BTC);

        Depth ETH_BTC = new Depth();
        ETH_BTC.setTimestamp(System.currentTimeMillis());
        ETH_BTC.setExchange("vnbig.com");
        ETH_BTC.setCurrency("ETH_BTC");
        List<Depth.PriceQuotation> ETH_BTC_asks = new ArrayList<>();
        List<Depth.PriceQuotation> ETH_BTC_bids = new ArrayList<>();
        //ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("413.59"), new BigDecimal("2.5076")));
        //ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("413.62"), new BigDecimal("0.5")));
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.049396"), new BigDecimal("15.5769")));
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.049394"), new BigDecimal("3")));
        ETH_BTC_asks.add(new Depth.PriceQuotation(new BigDecimal("0.049391"), new BigDecimal("0.1842")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.049376"), new BigDecimal("15.5937")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.049373"), new BigDecimal("46.9302")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.049353"), new BigDecimal("3")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.04935"), new BigDecimal("3.7895")));
        ETH_BTC_bids.add(new Depth.PriceQuotation(new BigDecimal("0.049338"), new BigDecimal("4.3223")));
        ETH_BTC.setAsks(ETH_BTC_asks);
        ETH_BTC.setBids(ETH_BTC_bids);
        depthes.put(currPairBA, ETH_BTC);

    }

    public static void main(String[] args) {

        CurrencyPair BA = new CurrencyPair("ETH_BTC", 6, 4);
        CurrencyPair CA = new CurrencyPair("PNT_BTC", 10, 2);
        CurrencyPair CB = new CurrencyPair("PNT_ETH", 8, 2);
        BA.setBuyFeeRate(new BigDecimal("0.001"));
        CA.setBuyFeeRate(new BigDecimal("0.001"));
        CB.setBuyFeeRate(new BigDecimal("0.001"));
        BA.setSellFeeRate(new BigDecimal("0.001"));
        CA.setSellFeeRate(new BigDecimal("0.001"));
        CB.setSellFeeRate(new BigDecimal("0.001"));


        new TriangleArbitrage()
                .setFailedSleepTime(10000L)
                .setMarketAvailableDuration(600L)
                .setCurrPairBA(BA)
                .setCurrPairCA(CA)
                .setCurrPairCB(CB)
                .setExchangeName("hadax.com")
                .setMaxQuantityPerOrder(new BigDecimal("20000"))
                .setMinQuantityPerOrder(new BigDecimal("10000"))
                .setCurrAmaxFreezeQuantity(new BigDecimal("0.001"))
                .setCurrBmaxFreezeQuantity(new BigDecimal("0.01"))
                .setCurrCmaxFreezeQuantity(new BigDecimal("500"))
                .run(null);
    }

}
