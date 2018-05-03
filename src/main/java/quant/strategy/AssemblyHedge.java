package quant.strategy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;

import org.apache.logging.log4j.Logger;

public class AssemblyHedge implements Strategy {

	private static final Logger logger = LogManager.getLogger(AssemblyHedge.class);
	
	private Map<String, List<String>> candidateCurrencyPairs = new HashMap<String, List<String>>();
	
	private Boolean init(Map<String, Object> parameters){
		Object candidateCurrencyPairs = parameters.get("candidateCurrencyPairs");
		if(null == candidateCurrencyPairs){
			logger.fatal("未指明备选交易对。");
			return false;
		}
		if(candidateCurrencyPairs instanceof HashMap){
			this.candidateCurrencyPairs = (Map<String, List<String>>) candidateCurrencyPairs;
		}
		return true;
	}
	
	
	public void run(Map<String, Object> parameters) {
		
		
		
		
	}

}
