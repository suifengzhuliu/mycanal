package canal.alibaba.otter.canal.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonValueProcessor;

/*
 JsonValueProcessor接口：
 每个属性的自定义序列化的基本接口。
 http://json-lib.sourceforge.net/apidocs/jdk15/net/sf/json/processors/JsonValueProcessor.html
 */
public class CommonProcessor implements JsonValueProcessor {

	public static final String Default_DATE_PATTERN = "yyyy-MM-dd";
	private DateFormat dateFormat;

	public CommonProcessor(String datePattern) {
		try {
			dateFormat = new SimpleDateFormat(datePattern);
		} catch (Exception e) {
			dateFormat = new SimpleDateFormat(Default_DATE_PATTERN);
		}
	}

	// processArrayValue:处理值并返回一个合适的JSON值。
	@Override
	public Object processArrayValue(Object value, JsonConfig jsonConfig) {
		// TODO Auto-generated method stub
		return process(value);
	}

	// processObjectValue:处理值并返回一个合适的JSON值。
	@Override
	public Object processObjectValue(String key, Object value, JsonConfig jsonConfig) {
		// TODO Auto-generated method stub
		return process(value);
	}

	private Object process(Object value) {
		// TODO Auto-generated method stub
		return dateFormat.format(value);
		/*
		 * format 将日期格式化为日期/时间字符串。
		 */
	}

}
