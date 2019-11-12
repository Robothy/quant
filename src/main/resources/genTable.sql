/**
 * 简单对冲策略进行中订单对的表
 */
create table LIVE_ORDER_PAIR(
	DATA_ID char(32) primary key,
	PLANTFORM varchar(30),
	CURRENCY varchar(15),
	BUY_ORDER_ID varchar(32),
	BUY_ORDER_PRICE numeric(20, 10),
	BUY_ORDER_QUANTITY numeric(20, 10),
	BUY_ORDER_STATUS varchar(15),
	CREATE_TIMESTAMP numeric(13),
	MODIFY_TIMESTAMP numeric(13),
	SELL_ORDER_ID varchar(32),
	SELL_ORDER_PRICE numeric(20, 10),
	SELL_ORDER_QUANTITY numeric(20, 10),
	SELL_ORDER_STATUS varchar(15)	
)

/**
 * 组合对冲策略订单记录表
 **/
create table ASSEMBLY_HEDGE_ORDER(
	DATA_ID char(32) primary key,	-- 数据ID
	HEDGE_ID char(32),				-- 对冲ID
	PLANTFORM varchar(30),			-- 平台
	CURRENCY_PAIR varchar(20),		-- 币种对
	QUOTE_CURRENCY varchar(10),		-- 计价币种
	BASE_CURRENCY varchar(10),		-- 基础币种
	ORDER_ID varchar(32),			-- 挂单ID
	ORDER_SIDE varchar(20),			-- 挂单方向
	ORDER_STATUS varchar(15),		-- 挂单状态
	ORDER_PRICE numeric(25, 15),	-- 挂单价格
	ORDER_QUANTITY numeric(25, 15),	-- 挂单数量
	TRANS_PRICE	numeric(25, 15),	-- 挂单成交价格
	FEE_RATE numeric(6, 5),			-- 手续费率
	CREATE_TIMESTAMP varchar(13),	-- 挂单创建时间
	MODIFY_TIMESTAMP varchar(13)	-- 挂单修改时间
)


/**
 * 波段对冲策略进行中订单对的表
 */
create table WAVE_HEDGE_ORDER(
	DATA_ID char(32) primary key,
	HEDGE_ID char(32),
	EXCHANGE_NAME varchar(30),
	CURRENCY_PAIR varchar(15),
	ORDER_SIDE varchar(4),
	ORDER_PRICE numeric(25, 15),
	ORDER_QUANTITY numeric(25, 15),
	ORDER_STATUS varchar(15),
	ORDER_ID varchar(32),
	FEE_RATE numeric(6,5),
	CREATE_TIMESTAMP numeric(13),
	MODIFY_TIMESTAMP numeric(13)
)


/**
 * 低价对冲策略订单对
 */

 drop  table TRIANGLE_ARBITRAGE_ORDER;
create table TRIANGLE_ARBITRAGE_ORDER(
	DATA_ID char(32) primary key,
	GROUP_ID char(32),  --交易组ID
	EXCHANGE_NAME varchar(30),  --交易所名称
	BASE_CURRENCY varchar(10),  --基础币种
	QUOTE_CURRENCY varchar(10), --计价币种
	ORDER_SIDE varchar(4),--挂单方向
	ORDER_PRICE numeric(25, 15),-- 挂单价格
	AVG_PRICE numeric (25, 15),   -- 平均价格
	ORDER_QUANTITY numeric(25, 15), -- 挂单数量
	ORDER_STATUS varchar(15), -- 订单状态
	ORDER_ID varchar(64), -- 订单ID
	FEE_RATE numeric(6,5), -- 费率
	CREATE_TIMESTAMP numeric(13), -- 创建时间戳
	MODIFY_TIMESTAMP numeric(13) -- 修改时间戳
)



create table TRIANGLE_ARBITRAGE_ORDER(
  DATA_ID char(32) primary key,
	HEDGE_ID char(32),
	EXCHANGE_NAME varchar(30),
	CURRENCY_PAIR varchar(15),
	ORDER_SIDE varchar(4),
	ORDER_PRICE numeric(35, 25),
	ORDER_QUANTITY numeric(35, 25),
	ORDER_STATUS varchar(15),
	ORDER_ID varchar(64),
	FEE_RATE numeric(6,5),
	CREATE_TIMESTAMP numeric(13),
	MODIFY_TIMESTAMP numeric(13)
)


