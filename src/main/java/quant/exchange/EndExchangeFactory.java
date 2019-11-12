package quant.exchange;

import java.io.*;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import exunion.exchange.Exchange;
import exunion.exchange.ExchangeFactory;

public class EndExchangeFactory {

    private static JSONObject exchangeConfiguraion = null;

    private static final Logger logger = LogManager.getLogger(EndExchangeFactory.class);

    // 静态初始化块，获取交易所配置文件，将配置文件处理成JSON对象
    static {

        // 交易所配置文件名称
        String exchangeConfigurationFileName = "exchanges.json";
        File configFile = new File(exchangeConfigurationFileName);
        Reader reader = null;
        try {
            reader = new FileReader(configFile);
        } catch (FileNotFoundException e) {
            logger.warn("未能在应用所在目录找到配置文件，尝试从jar包中读取交易所配置文件。");
        }
        String json = null;
        InputStream in = EndExchangeFactory.class.getClassLoader().getResourceAsStream(exchangeConfigurationFileName);
        if (reader != null) {
            try {
                json = IOUtils.toString(reader);
            } catch (IOException e) {
                logger.error(e);
            }
        } else if (null != in) {
            try {
                json = IOUtils.toString(in);
            } catch (IOException e) {
                logger.error(e);
            }
        } else {
            logger.warn("未能找到交易所的配置文件。");
            try {
                json = IOUtils.toString(in);
            } catch (IOException e) {
                logger.error("读取交易所配置文件时出现异常。", e);
            }
        }
        exchangeConfiguraion = JSON.parseObject(json);
    }

    /**
     * 获取一个带配置信息的交易所实例
     *
     * @param plantform 交易所名称
     * @return 一个交易所实例
     */
    public static Exchange newInstance(String plantform) {
        if (null == exchangeConfiguraion) {
            return ExchangeFactory.newInstance(plantform);
        }
        JSONObject exCfg = exchangeConfiguraion.getJSONObject(plantform);
        String key = exCfg.getString("key");
        String secret = exCfg.getString("secret");
        Boolean needProxy = exCfg.getBoolean("needProxy");
        return ExchangeFactory.newInstance(plantform, key, secret, needProxy);
    }

    private EndExchangeFactory() {
    }

}
