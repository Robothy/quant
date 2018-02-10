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