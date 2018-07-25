package canal.alibaba.otter.canal;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import canal.alibaba.otter.canal.common.CanalConstants;
import canal.alibaba.otter.canal.extract.MSDataParse;
import canal.alibaba.otter.canal.zk.ZookeeperPathUtils;
import canal.alibaba.otter.canal.zk.running.MSServerRunningListener;
import canal.alibaba.otter.canal.zk.running.MSServerRunningMonitor;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningData;

public class CanalMSClientLauncher {
	private final static Logger logger = LoggerFactory.getLogger(CanalMSClientLauncher.class);
	public static ConcurrentHashMap<String, MSDataParse> clientInstaceMap = new ConcurrentHashMap<String, MSDataParse>();
	private MSServerRunningMonitor monitor = null;

	protected String zkServers;
	private ZkClientx zkclientx;

	public CanalMSClientLauncher(Properties properties) {
		getProperty(properties, CanalConstants.CANAL_DBS);

		String destinationStr = getProperty(properties, CanalConstants.CANAL_DBS);
		String[] dbs = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT);

		zkServers = getProperty(properties, CanalConstants.CANAL_ZK_SERVERS);
		if (null == zkServers) {
			logger.error("zkServers is null ,  exit ");
			System.exit(0);
		}
		zkclientx = ZkClientx.getZkClient(zkServers);

		String ssid = getProperty(properties, CanalConstants.CANAL_SID);
		final long sid = Long.valueOf(ssid);
		final String ip = AddressUtils.getHostIp();
		ServerRunningData serverData = new ServerRunningData(sid, ip);
		monitor = new MSServerRunningMonitor(serverData);

		for (String db : dbs) {
			System.setProperty(CanalConstants.CANAL_DESTINATION_PROPERTY, db);
			String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(db));
			ApplicationContext applicationContext = new ClassPathXmlApplicationContext(springXml);
			MSDataParse instance = (MSDataParse) applicationContext.getBean("dbParser");
			instance.init(zkclientx);
			clientInstaceMap.put(db, instance);
		}

		monitor.setListener(new MSServerRunningListener() {

			@Override
			public void processStop() {

			}

			@Override
			public void processStart() {
				// 存储所有启动了的节点的信息
				try {
					if (zkclientx != null) {
						final String path = ZookeeperPathUtils.getClusterPath(ip + "-" + sid);
						initCid(path);
						zkclientx.subscribeStateChanges(new IZkStateListener() {

							public void handleStateChanged(KeeperState state) throws Exception {

							}

							public void handleNewSession() throws Exception {
								initCid(path);
							}
						});
					}
				} finally {
					MDC.remove(CanalConstants.MDC_DESTINATION);
				}

			}

			@Override
			public void processActiveExit() {

			}

			@Override
			public void processActiveEnter() {
				for (Map.Entry<String, MSDataParse> entry : clientInstaceMap.entrySet()) {
					try {
						MDC.put(CanalConstants.MDC_DESTINATION, entry.getKey());
						entry.getValue().start();
					} finally {
						MDC.remove(CanalConstants.MDC_DESTINATION);
					}
				}
			}
		});

		monitor.setZkClient(zkclientx);

	}

	public void start() {
		monitor.start();

		if (!monitor.isActive()) {
			logger.info("the server is standby node ,wait to become active node");
			try {
				monitor.waitForActive();
			} catch (InterruptedException e) {
				logger.error("the current thread wait to become active node , error", e);
			}

			logger.info("from standby node become active node");
		}
	}

	public void stop() {
		for (MSDataParse instance : clientInstaceMap.values()) {
			instance.stop();
		}
	}

	private String getProperty(Properties properties, String key) {
		return StringUtils.trim(properties.getProperty(StringUtils.trim(key)));
	}

	private void initCid(String path) {
		// logger.info("## init the canalId = {}", cid);
		// 初始化系统目录
		if (zkclientx != null) {
			try {
				zkclientx.createEphemeral(path);
			} catch (ZkNoNodeException e) {
				// 如果父目录不存在，则创建
				String parentDir = path.substring(0, path.lastIndexOf('/'));
				zkclientx.createPersistent(parentDir, true);
				zkclientx.createEphemeral(path);
			} catch (ZkNodeExistsException e) {
				// ignore
				// 因为第一次启动时创建了cid,但在stop/start的时可能会关闭和新建,允许出现NodeExists问题s
			}

		}
	}
}
