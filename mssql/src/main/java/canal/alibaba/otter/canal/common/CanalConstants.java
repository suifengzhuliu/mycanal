package canal.alibaba.otter.canal.common;

import java.text.MessageFormat;

/**
 * 启动常用变量
 * 
 * @author jianghang 2012-11-8 下午03:15:55
 * @version 1.0.0
 */
public class CanalConstants {

	public static final String MDC_DESTINATION = "msqql-destination";
	public static final String ROOT = "canal";
	public static final String CANAL_ID = ROOT + "." + "id";
	public static final String CANAL_IP = ROOT + "." + "ip";
	public static final String CANAL_PORT = ROOT + "." + "port";
	public static final String CANAL_ZKSERVERS = ROOT + "." + "zkServers";

	public static final String CANAL_DBS = ROOT + "." + "databases";
	public static final String CANAL_ZK_SERVERS = ROOT + "." + "zkServers";
	public static final String CANAL_SID = ROOT + "." + "sid";
	public static final String CANAL_AUTO_SCAN = ROOT + "." + "auto.scan";
	public static final String CANAL_AUTO_SCAN_INTERVAL = ROOT + "." + "auto.scan.interval";
	public static final String CANAL_CONF_DIR = ROOT + "." + "conf.dir";

	public static final String CANAL_DESTINATION_SPLIT = ",";
	public static final String GLOBAL_NAME = "global";

	public static final String INSTANCE_MODE_TEMPLATE = ROOT + "." + "instance.{0}.mode";
	public static final String INSTANCE_LAZY_TEMPLATE = ROOT + "." + "instance.{0}.lazy";
	public static final String INSTANCE_MANAGER_ADDRESS_TEMPLATE = ROOT + "." + "instance.{0}.manager.address";
	public static final String INSTANCE_SPRING_XML_TEMPLATE = ROOT + "." + "instance.{0}.spring.xml";

	public static final String CANAL_DESTINATION_PROPERTY = ROOT + ".instance.ms.destination";

	public static String getInstanceModeKey(String destination) {
		return MessageFormat.format(INSTANCE_MODE_TEMPLATE, destination);
	}

	public static String getInstanceManagerAddressKey(String destination) {
		return MessageFormat.format(INSTANCE_MANAGER_ADDRESS_TEMPLATE, destination);
	}

	public static String getInstancSpringXmlKey(String destination) {
		return MessageFormat.format(INSTANCE_SPRING_XML_TEMPLATE, destination);
	}

	public static enum SourceType {
		SQLSERVER, MYSQL
	}

	public static enum OperationType {
		INSERT, DELETE, SYNC
	}

	public static enum TaskRunResult {
		FAILED("失败", false), SUCCESS("成功", true);
		private String name;
		private boolean value;

		private TaskRunResult(String name, boolean value) {
			this.name = name;
			this.value = value;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public boolean getValue() {
			return value;
		}

		public void setValue(boolean value) {
			this.value = value;
		}
	}

	/**
	 * 替换敏感字段
	 *
	 */
	public static enum FilterColumn {
		CUT_HEAD(0, "截断首字符"), REPLACE_ALL(1, "替换所有字符为-"), REMOVE(2, "删除字段");
		private int index;
		private String name;

		private FilterColumn(int index, String name) {
			this.index = index;
			this.name = name;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}
}
