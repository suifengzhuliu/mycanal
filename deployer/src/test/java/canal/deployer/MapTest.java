package canal.deployer;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;

public class MapTest {
	public static void main(String[] args) {
		Map map = MigrateMap.makeComputingMap(new Function<String, Integer>() {

			public Integer apply(final String destination) {
				System.out.println("  ----   " + destination);
				return 1;
			}
		});

		System.out.println(map);
	}
}
