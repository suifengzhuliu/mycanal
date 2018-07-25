package canal.alibaba.otter.canal;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class CanalClientLauncher {
	private Map<String, CanalClientInstance> clientInstaceMap = new HashMap<String, CanalClientInstance>();

	public CanalClientLauncher(Properties properties) {
		getProperty(properties, CanalConstants.CANAL_DESTINATIONS);

		String destinationStr = getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
		String[] destinations = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT);

		for (String destination : destinations) {
			System.setProperty(CanalConstants.CANAL_DESTINATION_PROPERTY, destination);
			ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
			CanalClientInstance instance = (CanalClientInstance) applicationContext.getBean("instance");
			instance.init();
			clientInstaceMap.put(destination, instance);
		}

	}

	public void start() {
		for (CanalClientInstance instance : clientInstaceMap.values()) {
			instance.start();
		}

	}

	public void stop() {
		for (CanalClientInstance instance : clientInstaceMap.values()) {
			instance.stop();
		}
	}

	private String getProperty(Properties properties, String key) {
		return StringUtils.trim(properties.getProperty(StringUtils.trim(key)));
	}
}
