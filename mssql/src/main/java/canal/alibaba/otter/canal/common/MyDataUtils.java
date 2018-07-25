package canal.alibaba.otter.canal.common;

import org.apache.commons.lang.time.DateUtils;

public class MyDataUtils extends DateUtils {

	/**
	 * 获取两个时间的间隔分钟差
	 * 
	 * @param starttime
	 * @param endtime
	 * @return
	 */
	public static int getIntervalMinute(long starttime, long endtime) {
		return (int) (Math.abs(starttime - endtime) / (1000 * 60));
	}
}
