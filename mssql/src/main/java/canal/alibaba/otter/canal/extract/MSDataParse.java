package canal.alibaba.otter.canal.extract;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;

public interface MSDataParse {

	public void init(ZkClientx zkClientx);

	public void start();

	public void stop();

	public void parse();

	public boolean isOnlySubTable();
}
