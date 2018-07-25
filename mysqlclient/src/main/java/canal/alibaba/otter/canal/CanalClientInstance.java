package canal.alibaba.otter.canal;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import canal.alibaba.otter.canal.common.Constants.ReplaceColumn;
import canal.alibaba.otter.canal.filter.DataFilter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.common.utils.KafkaUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.Pair;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

public class CanalClientInstance {
	private final static Logger logger = LoggerFactory.getLogger(CanalClientInstance.class);
	private String destination;
	private String zkServer;
	private String columnReplace;
	private CanalConnector connector;
	private KafkaUtil kafkaUtil = KafkaUtil.instance();
	private Thread thread = null;
	private volatile boolean running = false;
	private String topic;
	private boolean dataFilter = false;
	/**
	 * 主要是针对分表的情况,member_1,member_2,这种情况，把表名统一成member,用正则的group匹配下.
	 * 所有的表都过滤的话成本有点大，所以加一个开关,默认false
	 */
	private boolean tableNameFilter = false;
	private String tableNamePattern;
	private List<Pattern> patternList = new ArrayList<Pattern>();
	private CanalAlarmHandler alarmHandler;
	private Map<String, Integer> replaceMap = new HashMap<String, Integer>();

	protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

		public void uncaughtException(Thread t, Throwable e) {
			logger.error("parse events has an error", e);
		}
	};

	protected static String context_format = null;
	protected static String row_format = null;
	protected static String transaction_format = null;
	protected static final String SEP = SystemUtils.LINE_SEPARATOR;
	static {
		context_format = SEP + "****************************************************" + SEP;
		context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
		context_format += "* Start : [{}] " + SEP;
		context_format += "* End : [{}] " + SEP;
		context_format += "****************************************************" + SEP;

		row_format = SEP + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms" + SEP;

		transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

	}

	public void init() {
		connector = CanalConnectors.newClusterConnector(zkServer, destination, "", "");

		if (StringUtils.isNotEmpty(columnReplace)) {
			try {
				// 格式：dbName.tableName.columnName:replaceType,dbName1.tableName1.columnName1:replaceType1
				String[] cols = columnReplace.split(",");
				for (String string : cols) {
					// 列以及列对应的替换规则
					String[] s1 = string.split(":");
					replaceMap.put(s1[0].toLowerCase(), Integer.parseInt(s1[1]));
				}
			} catch (NumberFormatException e) {
				logger.error("parse  columnReplace  error,the msg is ", e);
			}
		}

		if (tableNameFilter) {
			if (StringUtils.isNotEmpty(tableNamePattern)) {
				String[] ss = tableNamePattern.split(",");
				for (String pattern : ss) {
					patternList.add(Pattern.compile(pattern));
				}
			}
		}

	}

	public void start() {
		Assert.notNull(connector, "connector is null");
		thread = new Thread(new Runnable() {

			public void run() {
				process();
			}
		});

		thread.setUncaughtExceptionHandler(handler);
		thread.start();
		running = true;
	}

	protected void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}

		MDC.remove("destination");
	}

	protected void process() {
		int batchSize = 5 * 1024;
		while (running) {
			try {
				MDC.put("destination", destination);
				connector.connect();
				connector.subscribe();
				while (running) {
					Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
					long batchId = message.getId();
					int size = message.getEntries().size();
					if (batchId == -1 || size == 0) {
						// try {
						// Thread.sleep(1000);
						// } catch (InterruptedException e) {
						// }
					} else {
						// printSummary(message, batchId, size);
						printEntry(message.getEntries());
					}

					connector.ack(batchId); // 提交确认
					// connector.rollback(batchId); // 处理失败, 回滚数据
				}
			} catch (Exception e) {
				logger.error("process error!", e);
			} finally {
				connector.disconnect();
				MDC.remove("destination");
			}
		}
	}

	protected void printEntry(List<Entry> entrys) {
		for (Entry entry : entrys) {
			long executeTime = entry.getHeader().getExecuteTime();
			long delayTime = new Date().getTime() - executeTime;

			if (entry.getEntryType() == EntryType.ROWDATA) {
				RowChange rowChage = null;
				try {
					rowChage = RowChange.parseFrom(entry.getStoreValue());
				} catch (Exception e) {
					throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
				}

				EventType eventType = rowChage.getEventType();

				// logger.info(row_format, new Object[] {
				// entry.getHeader().getLogfileName(),
				// String.valueOf(entry.getHeader().getLogfileOffset()),
				// entry.getHeader().getSchemaName(),
				// entry.getHeader().getTableName(), eventType,
				// String.valueOf(entry.getHeader().getExecuteTime()),
				// String.valueOf(delayTime) });

				if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
					// logger.info(" sql ----> " + rowChage.getSql() + SEP);
					continue;
				}

				for (RowData rowData : rowChage.getRowDatasList()) {
					if (eventType == EventType.DELETE) {
						printColumn(rowData.getBeforeColumnsList(), eventType, entry.getHeader());
					} else if (eventType == EventType.INSERT) {
						printColumn(rowData.getAfterColumnsList(), eventType, entry.getHeader());
					} else {
						printColumn(rowData.getAfterColumnsList(), eventType, entry.getHeader());
					}
				}
			}
		}
	}

	protected void printColumn(List<Column> columns, EventType eventType, CanalEntry.Header header) {

		String tableName = header.getTableName();
		// 进行表名的正则匹配
		Matcher m = null;
		if (tableNameFilter) {
			// 目前是member_1这种规则，所以这里暂时写死，先取的正则匹配后group(1),以后若有变动，再设置不同的规则
			m = tableNameRegex(tableName);
			if (null != m) {
				tableName = m.group(1);
			}

		}

		JSONObject json = new JSONObject();
		json.put("sourceType", header.getSourceType());
		json.put("operationType", eventType);
		json.put("dbName", header.getSchemaName());
		json.put("tableName", tableName);
		json.put("executeTime", header.getExecuteTime());

		List<CanalEntry.Pair> propsListt = header.getPropsList();
		for (Pair pair : propsListt) {
			json.put(pair.getKey(), pair.getValue());
		}
		JSONObject data = new JSONObject();

		for (Column column : columns) {
			String value = column.getValue();
			value = dataFilter(header, column, value);
			data.put(column.getName(), value);
		}

		JSONArray ja = new JSONArray();
		ja.add(data);
		json.put("rowArr", ja);

		StringBuffer key = new StringBuffer();
		key.append(header.getSourceType());
		key.append("<>");
		key.append(json.getString("dbAddress"));
		key.append("<>");
		key.append(json.getString("dbPort"));
		key.append("<>");
		key.append(header.getSchemaName());
		key.append("<>");
		key.append(tableName);
		if (null != m) {
			key.append("<>");
			key.append(m.group(m.groupCount()));
		}

		kafkaUtil.sentData(topic, key.toString(), json.toString());
	}

	private Matcher tableNameRegex(String tableName) {

		for (Pattern pattern : patternList) {
			Matcher m = pattern.matcher(tableName);
			if (m.find()) {
				return m;
			}
		}

		return null;
	}

	/**
	 * 做数据清洗的工作
	 * 
	 * @param mysqlType
	 * @param value2
	 * @return
	 */
	private String dataFilter(CanalEntry.Header header, Column column, String value) {
		if (null == value) {
			return "";
		}
		String mysqlType = column.getMysqlType();
		// 文本类型
		if (mysqlType.indexOf("char") != -1 || mysqlType.indexOf("varchar") != -1 || mysqlType.indexOf("text") != -1) {
			value = value.trim();
			value = DataFilter.toSemiangle(value);
		}

		if (dataFilter) {
			String key = header.getSchemaName() + "." + header.getTableName() + "." + column.getName();
			key = key.toLowerCase();
			Integer replaceType = replaceMap.get(key);
			if (null != replaceType) {
				if (replaceType == ReplaceColumn.CUT_HEAD.getIndex()) {
					value = DataFilter.cutHead(value);
				}

				if (replaceType == ReplaceColumn.REPLACE_ALL.getIndex()) {
					value = DataFilter.replaceAll(value);
				}
			}
		}
		return value;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getZkServer() {
		return zkServer;
	}

	public void setZkServer(String zkServer) {
		this.zkServer = zkServer;
	}

	public CanalConnector getConnector() {
		return connector;
	}

	public void setConnector(CanalConnector connector) {
		this.connector = connector;
	}

	public String getColumnReplace() {
		return columnReplace;
	}

	public void setColumnReplace(String columnReplace) {
		this.columnReplace = columnReplace;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public boolean getDataFilter() {
		return dataFilter;
	}

	public void setDataFilter(boolean dataFilter) {
		this.dataFilter = dataFilter;
	}

	public boolean isTableNameFilter() {
		return tableNameFilter;
	}

	public void setTableNameFilter(boolean tableNameFilter) {
		this.tableNameFilter = tableNameFilter;
	}

	public String getTableNamePattern() {
		return tableNamePattern;
	}

	public void setTableNamePattern(String tableNamePattern) {
		this.tableNamePattern = tableNamePattern;
	}

	public CanalAlarmHandler getAlarmHandler() {
		return alarmHandler;
	}

	public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
		this.alarmHandler = alarmHandler;
	}

}
