package canal.example;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.alibaba.fastjson.JSON;

public class FastJsonTEST {
	public static void main(String[] args) {
		try {

			Map<String, List<Map<String, Object>>> map = new HashMap<String, List<Map<String, Object>>>();
			String s = FileUtils.readFileToString(new File("/tmp/json.txt"));
			System.out.println(s);
			map = JSON.parseObject(s, Map.class);
			System.out.println(map);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
