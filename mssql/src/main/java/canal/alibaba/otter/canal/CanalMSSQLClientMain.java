package canal.alibaba.otter.canal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;

public class CanalMSSQLClientMain {
	private final static Logger logger = LoggerFactory.getLogger(CanalMSSQLClientMain.class);

	public static void main(String[] args) {

		try {

			Properties properties = new Properties();
			ClassLoader cl = CanalMSSQLClientMain.class.getClassLoader();
			InputStream in = cl.getResourceAsStream("canal.properties");
			properties.load(in);
			final CanalMSClientLauncher client = new CanalMSClientLauncher(properties);
			client.start();
			logger.info("start canal client success");
			
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					logger.info("stop the mssql server");
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
