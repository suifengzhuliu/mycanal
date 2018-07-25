package com.alibaba.otter.canal.common.alarm;

public class AlarmConstants {
	public static enum AlarmTypeEnum {
		EMAIL(0), SMS(1), ALL(2);
		private int value;

		private AlarmTypeEnum(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

	}
}
