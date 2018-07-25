package canal.example;

import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Job_Schedule_Test3 {
	public static void main(String[] args) {
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		long delay = 0;
		long interval = 1;

		// 从现在开始 2 秒钟之后启动，每隔 1 秒钟执行一次
		service.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Test: " + Calendar.getInstance().getTime());

			}
		}, delay, interval, TimeUnit.SECONDS);
	}
}

class JobTask2 implements Runnable {
	public void run() {
		System.out.println("Test: " + Calendar.getInstance().getTime());
	}
}
