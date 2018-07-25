package canal.alibaba.otter.canal.extract.impl;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import canal.alibaba.otter.canal.common.CanalConstants.OperationType;
import canal.alibaba.otter.canal.common.CanalConstants.SourceType;
import canal.alibaba.otter.canal.extract.AbstractMSSqlParse;

import com.alibaba.fastjson.JSONObject;

/**
 * 抽取所有的数据，发送kafka
 * 
 * @author user
 *
 */

// @Configuration
// @PropertySource("classpath:databases/200.properties")
// @ConfigurationProperties(prefix = "canal.instance")
public class MSSQLAllParse extends AbstractMSSqlParse {

	private final static Logger logger = LoggerFactory.getLogger(MSSQLAllParse.class);

	private String tableName;

	@Override
	public void parse() {

		long start = System.currentTimeMillis();

		String sql = "select  *  from " + tableName + " with(nolock) ";

		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);

		StringBuffer key = new StringBuffer();
		key.append(SourceType.SQLSERVER);
		key.append("<>");
		key.append(dbAddress);
		key.append("<>");
		key.append(dbPort);
		key.append("<>");
		key.append(dbName);
		key.append("<>");
		key.append(tableName);
		
		Long now = System.currentTimeMillis();
		for (Map<String, Object> map : list) {
			JSONObject json = new JSONObject();
			json.put("sourceType", SourceType.SQLSERVER);
			json.put("type", OperationType.INSERT);
			json.put("dbName", dbName);
			json.put("dbAddress", dbAddress);
			json.put("dbPort", dbPort);
			json.put("tableName", tableName);
			json.put("executeTime", System.currentTimeMillis());
			json.put("row", map);
			kafkaUtil.sentData(kafkaTopic, key.toString(), json.toString());
			logger.debug("sent to kafka,the data is {}", json);
		}

		logger.info("parse table {} ,and sent all data  to kafka,the result size is {}, cost {} ms", tableName, list.size(), System.currentTimeMillis() - start);

	}

	public void stop() {
		super.stop();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}