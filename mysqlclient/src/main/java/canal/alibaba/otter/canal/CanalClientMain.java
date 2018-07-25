package canal.alibaba.otter.canal;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;

public class CanalClientMain {
	private final static Logger logger = LoggerFactory.getLogger(CanalClientMain.class);

	public static void main(String[] args) {

		try {

			Properties properties = new Properties();
			properties.load(CanalClientMain.class.getClassLoader().getResourceAsStream("canal.properties"));
			final CanalClientLauncher client = new CanalClientLauncher(properties);
			client.start();
			logger.info("start canal client success");

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					client.stop();
				}
			});
		} catch (BeansException e) {
			logger.error("main error ,the msg is", e);
		} catch (IOException e) {
			logger.error("main io  error ,the msg is", e);
		}

	}

}
