package canal.alibaba.otter.canal.extract.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import canal.alibaba.otter.canal.common.CanalConstants.FilterColumn;
import canal.alibaba.otter.canal.common.CanalConstants.OperationType;
import canal.alibaba.otter.canal.common.CanalConstants.SourceType;
import canal.alibaba.otter.canal.common.CanalConstants.TaskRunResult;
import canal.alibaba.otter.canal.common.Data2Json;
import canal.alibaba.otter.canal.common.MyDataUtils;
import canal.alibaba.otter.canal.extract.AbstractMSSqlParse;
import canal.alibaba.otter.canal.zk.ZookeeperPathUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 按照时间类型来解析，从一个大表中定时抽取所有的订单号，然后从N个子表中查出来对应的数据，发送kafka
 * 
 * @author user
 *
 */

// @Configuration
// @PropertySource("classpath:databases/200.properties")
// @ConfigurationProperties(prefix = "canal.instance")
public class MSSQLTimeParse extends AbstractMSSqlParse {

	private final static Logger logger = LoggerFactory.getLogger(MSSQLTimeParse.class);
	private ExecutorService executorService = Executors.newFixedThreadPool(5);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private String tableName;
	private String subTableList;
	// 每次发送数据限制，默认100
	private int sendLimit = 100;

	@Override
	public void parse() {
		long endTime = System.currentTimeMillis();

		long startTime;
		byte[] bytes = zkclientx.readData(ZookeeperPathUtils.getDestinationPath(destination));
		if (null == bytes) {
			startTime = DateUtils.addMinutes(new Date(endTime), -taskInverval).getTime();
			extractBigTableData(endTime, startTime);
		} else {
			startTime = Long.valueOf(new String(bytes));
			// 有一段时间程序停了或者
			if (MyDataUtils.getIntervalMinute(startTime, endTime) > taskInverval) {

				while (startTime <= endTime) {
					long eTime = DateUtils.addMinutes(new Date(startTime), taskInverval).getTime();
					if (eTime > endTime) {
						eTime = endTime;
					}
					extractBigTableData(eTime, startTime);
					startTime = DateUtils.addMinutes(new Date(startTime), taskInverval).getTime();
				}

			}

		}

	}

	private void extractBigTableData(long endTime, long startTime) {
		String sql = "select  *  from " + tableName + " where TimeStampLong >= ?  and TimeStampLong < ? order by TimeStampLong ";

		List<Map<String, Object>> list = null;
		try {
			list = jdbcTemplate.queryForList(sql, startTime, endTime);
		} catch (DataAccessException e1) {
			logger.error(String.format("query table %s error ,the error is ", tableName), e1);
			if (null != alarmHandler) {
				alarmHandler.sendAlarmElong("query " + tableName + "  error", e1.getMessage());
			}
			return;
		}

		if (null == list) {
			logger.info("query data from table {} ,the result list is null", tableName);
			return;
		} else {
			logger.info("parse table {} , the starttime is {},the endtime is {},the result size is {}, cost {} ms", tableName, startTime, endTime, list.size(),
					System.currentTimeMillis() - endTime);
		}

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

		// Set<Integer> orderIdSet = new HashSet<Integer>();
		JSONObject json = new JSONObject();
		json.put("sourceType", SourceType.SQLSERVER);
		json.put("operationType", OperationType.INSERT);
		json.put("dbName", dbName);
		json.put("dbAddress", dbAddress);
		json.put("dbPort", dbPort);
		json.put("tableName", tableName);
		json.put("executeTime", System.currentTimeMillis());

		JSONArray ja = new JSONArray();
		for (int i = 0; i < list.size(); i++) {
			// orderIdSet.add(Integer.valueOf(map.get("OrderID").toString()));
			if (i > 0 && i % sendLimit == 0) {
				json.put("rowArr", ja);
				kafkaUtil.sentData(kafkaTopic, key.toString(), json.toString());
				ja.clear();
			}
			ja.add(list.get(i));
		}
		if (ja.size() > 0) {
			json.put("rowArr", ja);
			kafkaUtil.sentData(kafkaTopic, key.toString(), json.toString());
			ja.clear();
		}

		if (null != subTableList && subTableList.length() > 0) {
			List<Future<TaskRunResult>> resultList = new ArrayList<Future<TaskRunResult>>();
			String[] subTables = subTableList.split(",");
			for (String subTable : subTables) {
				String[] ss = subTable.split(":");
				String subTableName = ss[0];
				String subTableOrderColumn = ss[1];
				Future<TaskRunResult> future = executorService.submit(new ExtractSubData(jdbcTemplate, dbName, dbAddress, dbPort, tableName, subTableName,
						subTableOrderColumn, startTime, endTime));
				resultList.add(future);
			}

			while (true) {
				try {
					boolean done = true;
					boolean res = true;
					for (Future<TaskRunResult> future : resultList) {
						done = done && future.isDone();
					}
					if (done) {
						for (Future<TaskRunResult> future : resultList) {
							res = res && future.get().getValue();
						}
						// 所有的子线程执行成功了，写入zookeeper，这次的更新时间点
						if (res) {
							zkclientx.writeData(ZookeeperPathUtils.getDestinationPath(destination), String.valueOf(endTime).getBytes());
						}
						break;
					}
				} catch (Exception e) {
					logger.error("wait  sub thread finish , error ,the msg is ", e);
					// 防止出现异常的时候无法退出死循环
					break;
				}
			}

		}

		/**
		 * 跨库抽取子表的数据,暂时先不用
		 */
		// for (Map.Entry<String, MSDataParse> entry :
		// CanalMSClientLauncher.clientInstaceMap.entrySet()) {
		// if (entry.getValue() instanceof MSSQLSubTableParse &&
		// entry.getValue().isOnlySubTable()) {
		// MSSQLSubTableParse subParse = (MSSQLSubTableParse) entry.getValue();
		// subParse.extractSubTableData(orderIdSet);
		// String subTableList = subParse.getSubTableList();
		// String[] subTables = subTableList.split(",");
		// for (String subTable : subTables) {
		// String[] ss = subTable.split(":");
		// String subTableName = ss[0];
		// String subTableOrderColumn = ss[1];
		//
		// executorService.submit(new
		// ExtractExtSubData(subParse.getNamedParameterJdbcTemplate(),
		// subParse.getDbName(), subParse.getDbAddress(),
		// subParse.getDbPort(), subTableName, subTableOrderColumn,
		// orderIdSet));
		// }

		// }
		// }
	}

	// class ExtractExtSubData implements Runnable {
	//
	// private String dbName;
	// private String dbAddress;
	// private Integer dbPort;
	// private String subTableName;
	// private String subTableOrderColumn;// 子表的订单id字段
	// private long begintime;
	// private long endtime;
	// private NamedParameterJdbcTemplate jdbcTemplate;
	// private Set<Integer> orderIdSet;
	//
	// public ExtractExtSubData(NamedParameterJdbcTemplate jdbcTemplate, String
	// dbName, String dbAddress, Integer dbPort, String subTableName,
	// String subTableOrderColumn, Set<Integer> orderIdSet) {
	// this.jdbcTemplate = jdbcTemplate;
	// this.dbName = dbName;
	// this.dbAddress = dbAddress;
	// this.dbPort = dbPort;
	// this.subTableName = subTableName;
	// this.subTableOrderColumn = subTableOrderColumn;
	// this.orderIdSet = orderIdSet;
	// }
	//
	// @Override
	// public void run() {
	// long parseStartTime = System.currentTimeMillis();
	// String sql = "select  r.* from " + subTableName +
	// "  r  WITH(NOLOCK)  where r." + subTableOrderColumn + "  in (:ids) ";
	//
	// int begin = 0;
	// int limit = 100;
	// int end = begin + limit;
	//
	// List<Integer> orderIdList = new ArrayList(orderIdSet);
	//
	// // 将主表的订单id列表拆分成多个子list，然后作为参数批量的去查询
	// while (true) {
	//
	// List<Integer> subOrderIdList = orderIdList.subList(begin, end >
	// orderIdList.size() ? orderIdList.size() : end);
	//
	// try {
	//
	//
	// JSONObject json = new JSONObject();
	// MapSqlParameterSource parameters = new MapSqlParameterSource();
	// parameters.addValue("ids", subOrderIdList);
	//
	// logger.info("begin to parse table {} ,the sql is {} ,the orderids is {} ",
	// subTableName, sql, subOrderIdList);
	//
	// List<Map<String, Object>> dataList = jdbcTemplate.queryForList(sql,
	// parameters);
	//
	// if (null != dataList && !dataList.isEmpty()) {
	//
	// json.put("sourceType", SourceType.SQLSERVER);
	// json.put("operationType", OperationType.INSERT);
	// json.put("dbName", dbName);
	// json.put("dbAddress", dbAddress);
	// json.put("dbPort", dbPort);
	// json.put("tableName", subTableName);
	// json.put("executeTime", System.currentTimeMillis());
	//
	// StringBuffer key = new StringBuffer();
	// key.append(SourceType.SQLSERVER);
	// key.append("<>");
	// key.append(dbAddress);
	// key.append("<>");
	// key.append(dbPort);
	// key.append("<>");
	// key.append(dbName);
	// key.append("<>");
	// key.append(subTableName);
	//
	// JSONArray ja = geneOrderIdList(subTableOrderColumn, subOrderIdList);
	// json.put("deleteByArr", ja);
	// json.put("rowArr", dataList);
	//
	// kafkaUtil.sentData(kafkaTopic, key.toString(), json.toString());
	//
	// long parseEndTime = System.currentTimeMillis();
	// logger.info("parse table {}  end and sent to kafka finished, the starttime is {},the endtime is {},the result size is {}, cost {} ms",
	// subTableName, begintime, endtime, dataList.size(), parseEndTime -
	// parseStartTime);
	// }
	//
	// } catch (Exception e) {
	// logger.error(String.format("parse table %s error ", subTableName), e);
	// }
	//
	// if (end > subOrderIdList.size()) {
	// break;
	// }
	// begin = begin + limit;
	// end = begin + limit;
	//
	// }
	//
	// //
	// logger.info("begin to parse table {} ,the sql is {} ,the orderids is {} ",
	// // subTableName, sql, orderIdSet);
	//
	// }
	//
	// private JSONArray geneOrderIdList(String subTableOrderColumn,
	// List<Integer> subOrderIdList) {
	// JSONArray ja = new JSONArray();
	// JSONObject jo = null;
	// for (Integer obj : subOrderIdList) {
	// jo = new JSONObject();
	// jo.put("orderId", obj);
	// ja.add(jo);
	// }
	// return ja;
	// }
	//
	// private JSONArray getOrderIdList(String key, List<Map<String, Object>>
	// sublist) {
	// JSONArray ja = new JSONArray();
	// // 用set去一下重复
	// Set set = new HashSet();
	// for (Map<String, Object> map : sublist) {
	// Object obj = map.get(key);
	// set.add(obj);
	// }
	//
	// JSONObject jo = null;
	// for (Object obj : set) {
	// jo = new JSONObject();
	// jo.put("orderId", obj);
	// ja.add(jo);
	// }
	//
	// return ja;
	// }
	// }

	public void stop() {
		super.stop();
		executorService.shutdown();
	}

	/**
	 * 抽取子表的数据
	 * 
	 * @author user
	 *
	 */
	class ExtractSubData implements Callable<TaskRunResult> {

		private String dbName;
		private String dbAddress;
		private Integer dbPort;
		private String subTableName;
		private String subTableOrderColumn;// 子表的订单id字段
		private long begintime;
		private long endtime;
		private JdbcTemplate jdbcTemplate;
		private String tableName;

		public ExtractSubData(JdbcTemplate jdbcTemplate, String dbName, String dbAddress, Integer dbPort, String tableName, String subTableName,
				String subTableOrderColumn, long begintime, long endtime) {
			this.jdbcTemplate = jdbcTemplate;
			this.dbName = dbName;
			this.dbAddress = dbAddress;
			this.dbPort = dbPort;
			this.tableName = tableName;
			this.subTableName = subTableName;
			this.subTableOrderColumn = subTableOrderColumn;
			this.begintime = begintime;
			this.endtime = endtime;
		}

		@Override
		public TaskRunResult call() {
			long parseStartTime = System.currentTimeMillis();
			String sql = "select  r.* from " + subTableName + "  r  WITH(NOLOCK)  left join " + tableName + " o   WITH(NOLOCK) on o.OrderID = r."
					+ subTableOrderColumn + "  where o.TimeStampLong >= ?  and o.TimeStampLong < ? order by o.OrderID, o.TimeStampLong  ";

			logger.info("begin to parse table {} ,the sql is {} ", subTableName, sql);
			try {
				net.sf.json.JSONObject json = new net.sf.json.JSONObject();
				List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, begintime, endtime);

				int begin = 0;
				int end = begin + sendLimit;
				if (null != list && !list.isEmpty()) {

					json.put("sourceType", SourceType.SQLSERVER);
					json.put("operationType", OperationType.INSERT);
					json.put("dbName", dbName);
					json.put("dbAddress", dbAddress);
					json.put("dbPort", dbPort);
					json.put("tableName", subTableName);
					json.put("executeTime", System.currentTimeMillis());

					StringBuffer key = new StringBuffer();
					key.append(SourceType.SQLSERVER);
					key.append("<>");
					key.append(dbAddress);
					key.append("<>");
					key.append(dbPort);
					key.append("<>");
					key.append(dbName);
					key.append("<>");
					key.append(subTableName);

					while (true) {

						List<Map<String, Object>> sublist = list.subList(begin, end > list.size() ? list.size() : end);
						net.sf.json.JSONArray ja = getOrderIdList(subTableOrderColumn, sublist);
						json.put("deleteByArr", ja);
						// json.put("rowArr",
						// net.sf.json.JSONArray.fromObject(sublist, config));
						json.put("rowArr", Data2Json.deal(sublist, config));
						if (ja.size() == 0 && sublist.size() != 0) {
							logger.error("select table {} error ,the join column is {},the first sublist is {}", subTableName, subTableOrderColumn,
									sublist.get(0));
							alarmHandler.sendAlarmElong("主表key为空", "关联子表" + subTableName + "查询，主表order为空，column is  " + subTableOrderColumn);
						}
						kafkaUtil.sentData(kafkaTopic, key.toString(), json.toString());
						// 添加等于，有可能正好查出来的条数是100的倍数
						if (end >= list.size()) {
							break;
						}
						begin = begin + sendLimit;
						end = begin + sendLimit;
					}

					long parseEndTime = System.currentTimeMillis();
					logger.info("parse table {}  end and sent to kafka finished, the starttime is {},the endtime is {},the result size is {}, cost {} ms",
							subTableName, begintime, endtime, list.size(), parseEndTime - parseStartTime);
				}

			} catch (Exception e) {
				logger.error(String.format("parse table %s  error ", subTableName), e);
				if (null != alarmHandler) {
					alarmHandler.sendAlarmElong("query " + subTableName + "  error", e.getMessage());
				}
				return TaskRunResult.FAILED;
			}
			return TaskRunResult.SUCCESS;
		}

		private net.sf.json.JSONArray getOrderIdList(String key, List<Map<String, Object>> sublist) {
			net.sf.json.JSONArray ja = new net.sf.json.JSONArray();
			// 用set去一下重复
			Set set = new HashSet();
			for (Map<String, Object> map : sublist) {
				Object obj = map.get(key);

				for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
					Map.Entry<String, Object> entry = (Map.Entry<String, Object>) iterator.next();
					String k = (subTableName + "." + entry.getKey()).toLowerCase();
					if (filterMap.containsKey(k)) {
						int value = filterMap.get(k);
						if (value == FilterColumn.REMOVE.getIndex()) {
							iterator.remove();
						}
					}

				}
				set.add(obj);
			}

			net.sf.json.JSONObject jo = null;
			for (Object obj : set) {
				jo = new net.sf.json.JSONObject();
				jo.put("orderId", obj);
				ja.add(jo);
			}

			return ja;
		}
	}

	class OrderChange {
		private int orderId;
		private long executeTime;

		public int getOrderId() {
			return orderId;
		}

		public void setOrderId(int orderId) {
			this.orderId = orderId;
		}

		public long getExecuteTime() {
			return executeTime;
		}

		public void setExecuteTime(long executeTime) {
			this.executeTime = executeTime;
		}

		@Override
		public String toString() {
			return "OrderChange [orderId=" + orderId + ", executeTime=" + executeTime + "]";
		}

	}

	public String getSubTableList() {
		return subTableList;
	}

	public void setSubTableList(String subTableList) {
		this.subTableList = subTableList;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getSendLimit() {
		return sendLimit;
	}

	public void setSendLimit(int sendLimit) {
		this.sendLimit = sendLimit;
	}

}
