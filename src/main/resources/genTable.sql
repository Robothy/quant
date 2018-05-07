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
	PLANTFROM varchar(30),			-- 平台
	CURRENCY varchar(15),			-- 币种
	ORDER_ID varchar(32),			-- 挂单ID
	ORDER_SIDE varchar(20),			-- 挂单方向
	ORDER_STATUS varchar(15),		-- 挂单状态
	ORDER_PRICE numeric(20, 10),	-- 挂单价格
	ORDER_QUANTITY numeric(20, 10),	-- 挂单数量
	TRANS_PRICE	numeric(20, 10),	-- 挂单成交价格
	CREATE_TIMESTAMP varchar(13),	-- 挂单创建时间
	MODIFY_TIMESTAMP varchar(13)	-- 挂单修改时间
)
