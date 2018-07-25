package canal.alibaba.otter.canal.extract;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.sf.json.JsonConfig;
import net.sf.json.util.PropertyFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import canal.alibaba.otter.canal.common.CommonProcessor;
import canal.alibaba.otter.canal.zk.ZookeeperPathUtils;

import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.common.utils.KafkaUtil;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;

public abstract class AbstractMSSqlParse implements MSDataParse {

	private final static Logger logger = LoggerFactory.getLogger(AbstractMSSqlParse.class);
	protected KafkaUtil kafkaUtil = KafkaUtil.instance();
	protected JdbcTemplate jdbcTemplate;
	protected String destination;
	protected String dbName;
	protected String dbAddress;
	protected Integer dbPort;
	protected Thread parseThread = null;
	protected String kafkaTopic;
	/**
	 * 要过滤的列,比如去掉敏感字符，删除过长的字段等
	 */
	protected String columnsFilter;

	protected Map<String, Integer> filterMap = new HashMap<String, Integer>();
	/**
	 * 定时任务执行的时间间隔
	 */
	protected int taskInverval;
	/**
	 * 定时任务启动的时候，延迟启动时间，主要是为了多个任务，错开执行
	 */
	protected int taskDelay;

	protected CanalAlarmHandler alarmHandler;

	/**
	 * 判断这个实例是否只是子表，如果为true，则这个实例不需要init和start。由主表进程在抽取数据的时候通过传递参数来调用执行
	 */
	protected boolean onlySubTable = false;

	private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
	protected ZkClientx zkclientx;

	protected JsonConfig config = new JsonConfig();

	public abstract void parse();

	public void init(ZkClientx zkclientx) {
		this.zkclientx = zkclientx;

		// 初始化系统目录
		this.zkclientx.createPersistent(ZookeeperPathUtils.getDestinationPath(destination), true);

		if (null != columnsFilter && columnsFilter.length() > 0) {
			try {
				// 格式：tableName.columnName:replaceType,tableName1.columnName1:replaceType1
				String[] cols = columnsFilter.split(",");
				for (String string : cols) {
					// 列以及列对应的替换规则
					String[] s1 = string.split(":");
					filterMap.put(s1[0].toLowerCase(), Integer.parseInt(s1[1]));
				}
			} catch (NumberFormatException e) {
				logger.error("parse  columnfilter  error,the msg is ", e);
			}
		}

		config.registerJsonValueProcessor(Date.class, new CommonProcessor("yyyy-MM-dd hh:mm:ss"));
		config.registerJsonValueProcessor(java.sql.Timestamp.class, new CommonProcessor("yyyy-MM-dd hh:mm:ss"));

		PropertyFilter filter = new PropertyFilter() {
			public boolean apply(Object object, String fieldName, Object fieldValue) {
				return null == fieldValue || "".equals(fieldValue);
			}
		};
		config.setJsonPropertyFilter(filter);

	}

	public void start() {
		service.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					parse();
				} catch (Exception e) {
					logger.error("parse data error , the msg is ", e);
				}
			}
		}, taskDelay, taskInverval, TimeUnit.MINUTES);
	}

	public boolean isOnlySubTab() {
		return this.onlySubTable;
	}

	public void stop() {
		service.shutdown();
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbAddress() {
		return dbAddress;
	}

	public void setDbAddress(String dbAddress) {
		this.dbAddress = dbAddress;
	}

	public Integer getDbPort() {
		return dbPort;
	}

	public void setDbPort(Integer dbPort) {
		this.dbPort = dbPort;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public int getTaskInverval() {
		return taskInverval;
	}

	public void setTaskInverval(int taskInverval) {
		this.taskInverval = taskInverval;
	}

	public int getTaskDelay() {
		return taskDelay;
	}

	public void setTaskDelay(int taskDelay) {
		this.taskDelay = taskDelay;
	}

	public boolean isOnlySubTable() {
		return onlySubTable;
	}

	public void setOnlySubTable(boolean onlySubTable) {
		this.onlySubTable = onlySubTable;
	}

	public CanalAlarmHandler getAlarmHandler() {
		return alarmHandler;
	}

	public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
		this.alarmHandler = alarmHandler;
	}

	public ZkClientx getZkclientx() {
		return zkclientx;
	}

	public void setZkclientx(ZkClientx zkclientx) {
		this.zkclientx = zkclientx;
	}

	public String getColumnsFilter() {
		return columnsFilter;
	}

	public void setColumnsFilter(String columnsFilter) {
		this.columnsFilter = columnsFilter;
	}

}
