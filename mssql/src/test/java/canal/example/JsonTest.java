package canal.example;

import com.alibaba.fastjson.JSONArray;

public class JsonTest {

	public static void main(String[] args) {
		JSONArray ja = new JSONArray();
		ja.add("a");
		System.out.println(ja);
		ja.clear();
		System.out.println(ja.size());

		System.out.println(100 % 100);
	}

}
