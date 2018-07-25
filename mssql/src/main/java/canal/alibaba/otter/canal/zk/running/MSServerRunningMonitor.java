package canal.alibaba.otter.canal.zk.running;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import canal.alibaba.otter.canal.zk.ZookeeperPathUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningData;

/**
 * 针对server的running节点控制
 * 
 * @author jianghang 2012-11-22 下午02:59:42
 * @version 1.0.0
 */
public class MSServerRunningMonitor extends AbstractCanalLifeCycle {

	private static final Logger logger = LoggerFactory.getLogger(MSServerRunningMonitor.class);
	private ZkClientx zkClient;
	private IZkDataListener dataListener;
	private BooleanMutex mutex = new BooleanMutex(false);
	private volatile boolean release = false;
	// 当前服务节点状态信息
	private ServerRunningData serverData;
	// 当前实际运行的节点状态信息
	private volatile ServerRunningData activeData;
	private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);
	private int delayTime = 5;
	private MSServerRunningListener listener;

	private volatile boolean isActive = false;

	public MSServerRunningMonitor(ServerRunningData serverData) {
		this();
		this.serverData = serverData;
	}

	public MSServerRunningMonitor() {
		// 创建父节点
		dataListener = new IZkDataListener() {

			public void handleDataChange(String dataPath, Object data) throws Exception {
				ServerRunningData runningData = JsonUtils.unmarshalFromByte((byte[]) data, ServerRunningData.class);
				if (!isMine(runningData)) {
					mutex.set(false);
				}

				if (!runningData.isActive() && isMine(runningData)) { // 说明出现了主动释放的操作，并且本机之前是active
					release = true;
					releaseRunning();// 彻底释放mainstem
				}

				activeData = (ServerRunningData) runningData;
			}

			public void handleDataDeleted(String dataPath) throws Exception {
				mutex.set(false);
				if (!release && activeData != null && isMine(activeData)) {
					// 如果上一次active的状态就是本机，则即时触发一下active抢占
					initRunning();
				} else {
					// 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
					delayExector.schedule(new Runnable() {

						public void run() {
							initRunning();
						}
					}, delayTime, TimeUnit.SECONDS);
				}
			}

		};

	}

	public void init() {
		processStart();
	}

	public void start() {
		super.start();
		processStart();
		// 如果需要尽可能释放instance资源，不需要监听running节点，不然即使stop了这台机器，另一台机器立马会start
		String path = ZookeeperPathUtils.MS_RUNNING_NODE;
		zkClient.subscribeDataChanges(path, dataListener);
		initRunning();
	}

	public void release() {
		if (zkClient != null) {
			releaseRunning(); // 尝试一下release
		} else {
			processActiveExit(); // 没有zk，直接启动
		}
	}

	public void stop() {
		super.stop();

		if (zkClient != null) {
			String path = ZookeeperPathUtils.MS_RUNNING_NODE;
			zkClient.unsubscribeDataChanges(path, dataListener);

			releaseRunning(); // 尝试一下release
		} else {
			processActiveExit(); // 没有zk，直接启动
		}
		processStop();
	}

	private void initRunning() {
		if (!isStart()) {
			return;
		}

		String path = ZookeeperPathUtils.MS_RUNNING_NODE;
		// 序列化
		byte[] bytes = JsonUtils.marshalToByte(serverData);
		try {
			mutex.set(false);
			zkClient.create(path, bytes, CreateMode.EPHEMERAL);
			activeData = serverData;
			processActiveEnter();// 触发一下事件
			isActive = true;
			mutex.set(true);
		} catch (ZkNodeExistsException e) {
			bytes = zkClient.readData(path, true);
			if (bytes == null) {// 如果不存在节点，立即尝试一次
				initRunning();
			} else {
				activeData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
			}
		} catch (ZkNoNodeException e) {
			zkClient.createPersistent(ZookeeperPathUtils.OTTER_ROOT_NODE, true); // 尝试创建父节点
			initRunning();
		}
	}

	public boolean isActive() {
		return this.isActive;
	}

	/**
	 * 阻塞等待自己成为active，如果自己成为active，立马返回
	 * 
	 * @throws InterruptedException
	 */
	public void waitForActive() throws InterruptedException {
		mutex.get();
	}

	/**
	 * 检查当前的状态
	 */
	public boolean check() {
		String path = ZookeeperPathUtils.MS_RUNNING_NODE;
		try {
			byte[] bytes = zkClient.readData(path);
			ServerRunningData eventData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
			activeData = eventData;// 更新下为最新值
			// 检查下nid是否为自己
			boolean result = isMine(activeData);
			if (!result) {
				logger.warn("canal is running in node[{}] , but not in node[{}]", activeData.getCid(), serverData.getCid());
			}
			return result;
		} catch (ZkNoNodeException e) {
			logger.warn("canal is not run any in node");
			return false;
		} catch (ZkInterruptedException e) {
			logger.warn("canal check is interrupt");
			Thread.interrupted();// 清除interrupt标记
			return check();
		} catch (ZkException e) {
			logger.warn("canal check is failed");
			return false;
		}
	}

	private boolean releaseRunning() {
		if (check()) {
			String path = ZookeeperPathUtils.MS_RUNNING_NODE;
			zkClient.delete(path);
			mutex.set(false);
			processActiveExit();
			return true;
		}

		return false;
	}

	// ====================== helper method ======================

	private boolean isMine(ServerRunningData address) {
		return address.getAddress().equals(serverData.getAddress()) && serverData.getCid().equals(address.getCid());
	}

	private void processStart() {
		if (listener != null) {
			try {
				listener.processStart();
			} catch (Exception e) {
				logger.error("processStart failed", e);
			}
		}
	}

	private void processStop() {
		if (listener != null) {
			try {
				listener.processStop();
			} catch (Exception e) {
				logger.error("processStop failed", e);
			}
		}
	}

	private void processActiveEnter() {
		if (listener != null) {
			try {
				listener.processActiveEnter();
			} catch (Exception e) {
				logger.error("processActiveEnter failed", e);
			}
		}
	}

	private void processActiveExit() {
		if (listener != null) {
			try {
				listener.processActiveExit();
			} catch (Exception e) {
				logger.error("processActiveExit failed", e);
			}
		}
	}

	public void setListener(MSServerRunningListener listener) {
		this.listener = listener;
	}

	// ===================== setter / getter =======================

	public void setDelayTime(int delayTime) {
		this.delayTime = delayTime;
	}

	public void setServerData(ServerRunningData serverData) {
		this.serverData = serverData;
	}

	public void setZkClient(ZkClientx zkClient) {
		this.zkClient = zkClient;
	}

}
