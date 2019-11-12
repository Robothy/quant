package exunion.test;

import java.math.BigDecimal;

import quant.exchange.EndExchangeFactory;

public class ExunionTest {

	public static void main(String[] args) {

		System.out.println("hello");

		//System.out.println(EndExchangeFactory.newInstance("bit-z.com").getAccount());
		//System.out.println(EndExchangeFactory.newInstance("hadax.com").getAccount().getBalances());
		System.out.println(EndExchangeFactory.newInstance("hadax.com").order("BUY", "ETH_BTC", new BigDecimal("0.001"), new BigDecimal("0.001")));

		//System.out.println(EndExchangeFactory.newInstance("bit-z.com").getDepth("INC_ETH"));
		//System.out.println(EndExchangeFactory.newInstance("bit-z.com").getDepth("INC_ETH"));
		//System.out.println(EndExchangeFactory.newInstance("vnbig.com").getDepth("INC_ETH"));
		//System.out.println(EndExchangeFactory.newInstance("vnbig.com").getAccount().getBalances());
		//System.out.println(EndExchangeFactory.newInstance("vnbig.com").order("SELL", "INC_ETH", new BigDecimal("10"), new BigDecimal("0.1")));
		//System.out.println(EndExchangeFactory.newInstance("vnbig.com").getOrder("INC_ETH", "778135664406814720"));
	}

}
