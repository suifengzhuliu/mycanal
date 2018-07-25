package canal.alibaba.otter.canal.common;

import java.util.List;

import net.sf.json.JsonConfig;

public class Data2Json {

	public static synchronized net.sf.json.JSONArray deal(List list, JsonConfig config) {
		return net.sf.json.JSONArray.fromObject(list, config);
	}
}
