package canal.alibaba.otter.canal.common;

public class Constants {
	/**
	 * 替换敏感字段
	 *
	 */
	public static enum ReplaceColumn {
		CUT_HEAD(0, "截断首字符"), REPLACE_ALL(1, "替换所有字符为-");
		private int index;
		private String name;

		private ReplaceColumn(int index, String name) {
			this.index = index;
			this.name = name;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}
}
