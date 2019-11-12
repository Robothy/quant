package quant.utils;

import exunion.exchange.Exchange;
import quant.exchange.EndExchangeFactory;

import java.util.ArrayList;
import java.util.List;

public class NetworkTest {

    private static Exchange exchange = EndExchangeFactory.newInstance("hadax.com");

    private static List<String> currPairs = new ArrayList<>();




    public static void main(String[] args){

        currPairs.add("PNT_ETH");
        currPairs.add("PNT_BTC");
        currPairs.add("ETH_BTC");

        while (true){

            for (int i=0; i<currPairs.size(); i++){
                Long singleBigin = System.currentTimeMillis();
                exchange.getDepth(currPairs.get(i));
                Long singleEnd = System.currentTimeMillis();
                System.out.println(currPairs.get(i) + ": " + (singleEnd - singleBigin));
            }
            System.out.println();

            Long parallelBegin = System.currentTimeMillis();
            currPairs.parallelStream().forEach(e -> {
                Long begin = System.currentTimeMillis();
                exchange.getDepth(e);
                Long end = System.currentTimeMillis();
                System.out.println("parallel " + e + " " + (end - begin));
            }

        );
            Long parallelEnd = System.currentTimeMillis();
            System.out.println("parallel: " + (parallelEnd - parallelBegin));
            System.out.println();

        }
    }

}
