package canal.alibaba.otter.canal.zk;

import java.text.MessageFormat;

/**
 * 存储结构：
 * 
 * <pre>
 * /otter
 *    mssql
 *      parseposition
 *        dest1
 *        dest2
 * 
 * </pre>
 * 
 * @version 1.0.0
 */
public class ZookeeperPathUtils {

	public static final String ZOOKEEPER_SEPARATOR = "/";

	public static final String OTTER_ROOT_NODE = ZOOKEEPER_SEPARATOR + "otter";

	public static final String MS_ROOT_NODE = OTTER_ROOT_NODE + ZOOKEEPER_SEPARATOR + "mssql";

	public static final String MS_RUNNING_NODE = MS_ROOT_NODE + ZOOKEEPER_SEPARATOR + "running";

	public static final String MS_ROOT_CLUSTER = MS_ROOT_NODE + ZOOKEEPER_SEPARATOR + "cluster";

	public static final String DESTINATION_ROOT_NODE = MS_ROOT_NODE + ZOOKEEPER_SEPARATOR + "parseposition";

	public static final String DESTINATION_NODE = DESTINATION_ROOT_NODE + ZOOKEEPER_SEPARATOR + "{0}";

	public static final String MS_CLUSTER_PATH = MS_ROOT_CLUSTER + ZOOKEEPER_SEPARATOR + "{0}";

	public static String getDestinationPath(String destinationName) {
		return MessageFormat.format(DESTINATION_NODE, destinationName);
	}

	public static String getClusterPath(String info) {
		return MessageFormat.format(MS_CLUSTER_PATH, info);
	}

}
