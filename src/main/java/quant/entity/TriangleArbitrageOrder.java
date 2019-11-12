package quant.entity;

import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Objects;

@Entity
@Table(name = "TRIANGLE_ARBITRAGE_ORDER", catalog = "quant")
public class TriangleArbitrageOrder {
    private String dataId;
    private String groupId;
    private String exchangeName;
    private String baseCurrency;
    private String quoteCurrency;
    private String orderSide;
    private BigDecimal orderPrice;
    private BigDecimal avgPrice;
    private BigDecimal orderQuantity;
    private String orderStatus;
    private String orderId;
    private BigDecimal feeRate;
    private Long createTimestamp;
    private Long modifyTimestamp;

    @Id
    @GenericGenerator(name="uuid", strategy="uuid")
    @GeneratedValue(generator="uuid")
    @Column(name = "DATA_ID", unique = true, nullable = false, length = 32)
    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    @Basic
    @Column(name = "GROUP_ID")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Basic
    @Column(name = "EXCHANGE_NAME")
    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    @Basic
    @Column(name = "BASE_CURRENCY")
    public String getBaseCurrency() {
        return baseCurrency;
    }

    public void setBaseCurrency(String baseCurrency) {
        this.baseCurrency = baseCurrency;
    }

    @Basic
    @Column(name = "QUOTE_CURRENCY")
    public String getQuoteCurrency() {
        return quoteCurrency;
    }

    public void setQuoteCurrency(String quoteCurrency) {
        this.quoteCurrency = quoteCurrency;
    }

    @Basic
    @Column(name = "ORDER_SIDE")
    public String getOrderSide() {
        return orderSide;
    }

    public void setOrderSide(String orderSide) {
        this.orderSide = orderSide;
    }

    @Basic
    @Column(name = "ORDER_PRICE")
    public BigDecimal getOrderPrice() {
        return orderPrice;
    }

    public void setOrderPrice(BigDecimal orderPrice) {
        this.orderPrice = orderPrice;
    }

    @Basic
    @Column(name = "AVG_PRICE")
    public BigDecimal getAvgPrice() {
        return avgPrice;
    }

    public void setAvgPrice(BigDecimal avgPrice) {
        this.avgPrice = avgPrice;
    }

    @Basic
    @Column(name = "ORDER_QUANTITY")
    public BigDecimal getOrderQuantity() {
        return orderQuantity;
    }

    public void setOrderQuantity(BigDecimal orderQuantity) {
        this.orderQuantity = orderQuantity;
    }

    @Basic
    @Column(name = "ORDER_STATUS")
    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    @Basic
    @Column(name = "ORDER_ID")
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @Basic
    @Column(name = "FEE_RATE")
    public BigDecimal getFeeRate() {
        return feeRate;
    }

    public void setFeeRate(BigDecimal feeRate) {
        this.feeRate = feeRate;
    }

    @Basic
    @Column(name = "CREATE_TIMESTAMP")
    public Long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(Long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    @Basic
    @Column(name = "MODIFY_TIMESTAMP")
    public Long getModifyTimestamp() {
        return modifyTimestamp;
    }

    public void setModifyTimestamp(Long modifyTimestamp) {
        this.modifyTimestamp = modifyTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TriangleArbitrageOrder that = (TriangleArbitrageOrder) o;
        return Objects.equals(dataId, that.dataId) &&
                Objects.equals(groupId, that.groupId) &&
                Objects.equals(exchangeName, that.exchangeName) &&
                Objects.equals(baseCurrency, that.baseCurrency) &&
                Objects.equals(quoteCurrency, that.quoteCurrency) &&
                Objects.equals(orderSide, that.orderSide) &&
                Objects.equals(orderPrice, that.orderPrice) &&
                Objects.equals(avgPrice, that.avgPrice) &&
                Objects.equals(orderQuantity, that.orderQuantity) &&
                Objects.equals(orderStatus, that.orderStatus) &&
                Objects.equals(orderId, that.orderId) &&
                Objects.equals(feeRate, that.feeRate) &&
                Objects.equals(createTimestamp, that.createTimestamp) &&
                Objects.equals(modifyTimestamp, that.modifyTimestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dataId, groupId, exchangeName, baseCurrency, quoteCurrency, orderSide, orderPrice, avgPrice, orderQuantity, orderStatus, orderId, feeRate, createTimestamp, modifyTimestamp);
    }
}
