package canal.alibaba.otter.canal.common;

public class DataFilter {
	/**
	 * 全角空格为12288，半角空格为32， 其他字符半角(33-126)与全角(65281-65374)的对应关系是：均相差65248
	 *
	 * 将字符串中的全角字符转为半角
	 * 
	 * @param src
	 *            要转换的包含全角的任意字符串
	 * @return 转换之后的字符串
	 */
	public static String toSemiangle(String src) {
		char[] c = src.toCharArray();
		for (int index = 0; index < c.length; index++) {
			if (c[index] == 12288) {// 全角空格
				c[index] = (char) 32;
			} else if (c[index] > 65280 && c[index] < 65375) {// 其他全角字符
				c[index] = (char) (c[index] - 65248);
			}
		}
		return String.valueOf(c);
	}

	/**
	 * 替换手机号中间四位为星号
	 * 
	 * @param tel
	 *            11位数字的手机号
	 * @return 替换之后的值
	 */
	public static String repalcePhone(String tel) {
		if (null == tel) {
			return "";
		}
		return tel.replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2");
	}

	/**
	 * 替换手机号中间四位为星号
	 * 
	 * @param tel
	 *            11位数字的手机号
	 * @return 替换之后的值
	 */
	public static String repalceAll(String str) {
		if (null == str) {
			return "";
		}
		return replaceAction(str, "(?<=\\d{3})\\d(?=\\d{4})");
	}

	/**
	 * 实际替换动作
	 *
	 * @param username
	 *            username
	 * @param regular
	 *            正则
	 * @return
	 */
	private static String replaceAction(String username, String regular) {
		return username.replaceAll(regular, "*");
	}

	/**
	 * 身份证号替换，保留前四位和后四位
	 *
	 * 如果身份证号为空 或者 null ,返回null ；否则，返回替换后的字符串；
	 *
	 * @param idCard
	 *            身份证号
	 * @return
	 */
	public static String idCardReplaceWithStar(String idCard) {

		if (null == idCard || idCard.isEmpty()) {
			return "";
		} else {
			return replaceAction(idCard, "(?<=\\d{4})\\d(?=\\d{4})");
		}
	}

	/**
	 * 银行卡替换，保留后四位
	 *
	 * 如果银行卡号为空 或者 null ,返回null ；否则，返回替换后的字符串；
	 *
	 * @param bankCard
	 *            银行卡号
	 * @return
	 */
	public static String bankCardReplaceWithStar(String bankCard) {

		if (null == bankCard || bankCard.isEmpty()) {
			return null;
		} else {
			return replaceAction(bankCard, "(?<=\\d{0})\\d(?=\\d{4})");
		}
	}
}
