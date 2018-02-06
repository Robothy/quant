create table LIVE_ORDER_PAIRS(
	DATA_ID int not null identity(1,1) primary key,
	PLANTFORM varchar(30),
	CURRENCY varchar(15),
	BUY_ORDER_ID varchar(32),
	BUY_ORDER_PRICE numeric(20, 10),
	BUY_ORDER_QUANTITY numeric(20, 10),
	BUY_ORDER_STATUS varchar(15),
	CREATE_TIMESTAMP numeric(10),
	MODIFY_TIMESTAMP numeric(10),
	SELL_ORDER_ID varchar(32),
	SELL_ORDER_PRICE numeric(20, 10),
	SELL_ORDER_QUANTITY numeric(20, 10),
	SELL_ORDER_STATUS varchar(15)
	
)