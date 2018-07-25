package com.alibaba.otter.canal.filter;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.filter.aviater.AviaterELFilter;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.filter.aviater.AviaterSimpleFilter;
import com.alibaba.otter.canal.protocol.CanalEntry;

public class AviaterFilterTest {

	@Test
	public void test_simple() {
		AviaterSimpleFilter filter = new AviaterSimpleFilter("s1.t1,s2.t2");
		boolean result = filter.filter("s1.t1");
		Assert.assertEquals(true, result);

		result = filter.filter("s1.t2");
		Assert.assertEquals(false, result);

		result = filter.filter("");
		Assert.assertEquals(true, result);

		result = filter.filter("s1.t1,s2.t2");
		Assert.assertEquals(false, result);

		result = filter.filter("s2.t2");
		Assert.assertEquals(true, result);
	}

	@Test
	public void test_regex() {
		AviaterRegexFilter filter = new AviaterRegexFilter("s1\\..*,s2\\..*");
		boolean result = filter.filter("s1.t1");
		Assert.assertEquals(true, result);

		result = filter.filter("s1.t2");
		Assert.assertEquals(true, result);

		result = filter.filter("");
		Assert.assertEquals(true, result);

		result = filter.filter("s12.t1");
		Assert.assertEquals(false, result);

		result = filter.filter("s2.t2");
		Assert.assertEquals(true, result);

		result = filter.filter("s3.t2");
		Assert.assertEquals(false, result);

		AviaterRegexFilter filter2 = new AviaterRegexFilter("s1\\..*,s2.t1");

		result = filter2.filter("s1.t1");
		Assert.assertEquals(true, result);

		result = filter2.filter("s1.t2");
		Assert.assertEquals(true, result);

		result = filter2.filter("s2.t1");
		Assert.assertEquals(true, result);

		AviaterRegexFilter filter3 = new AviaterRegexFilter("foooo,f.*t");

		result = filter3.filter("fooooot");
		Assert.assertEquals(true, result);

		AviaterRegexFilter filter4 = new AviaterRegexFilter("otter2.otter_stability1|otter1.otter_stability1|retl.retl_mark|retl.retl_buffer|retl.xdual");
		result = filter4.filter("otter1.otter_stability1");
		Assert.assertEquals(true, result);
	}

	@Test
	public void testMany() {
		AviaterRegexFilter filter2 = new AviaterRegexFilter(
				"pb_member\\.member_[\\d]+,pb_member\\.member_ext_[\\d]+,pb_member.member_message_config,pb_member.member_advanced_reginfo,pb_member.company_department_dict,pb_member.proxy,pb_member.proxy_register_member,pb_member.proxy_category_rela,pb_member.proxy_contact,pb_member.ap,pb_member.default_cards,pb_member.common_cards");

		boolean result = filter2.filter("pb_member.member_1");
		Assert.assertEquals(true, result);

		result = filter2.filter("pb_member.member_aa");
		Assert.assertEquals(false, result);

		result = filter2.filter("pb_member.member_ext_12");
		Assert.assertEquals(true, result);

		result = filter2.filter("pb_member.member_ext_aa");
		Assert.assertEquals(false, result);

		result = filter2.filter("pb_member.member_message_config");
		Assert.assertEquals(true, result);
		
		result = filter2.filter("pb_member.ap");
		Assert.assertEquals(true, result);

	}

	@Test
	public void testDisordered() {
		AviaterRegexFilter filter = new AviaterRegexFilter("u\\..*,uvw\\..*,uv\\..*,a\\.x,a\\.xyz,a\\.xy,abc\\.x,abc\\.xyz,abc\\.xy,ab\\.x,ab\\.xyz,ab\\.xy");

		boolean result = filter.filter("u.abc");
		Assert.assertEquals(true, result);

		result = filter.filter("ab.x");
		Assert.assertEquals(true, result);

		result = filter.filter("ab.xyz1");
		Assert.assertEquals(false, result);

		result = filter.filter("abc.xyz");
		Assert.assertEquals(true, result);

		result = filter.filter("uv.xyz");
		Assert.assertEquals(true, result);

	}

	@Test
	public void test_el() {
		AviaterELFilter filter = new AviaterELFilter("str(entry.entryType) == 'ROWDATA'");

		CanalEntry.Entry.Builder entry = CanalEntry.Entry.newBuilder();
		entry.setEntryType(CanalEntry.EntryType.ROWDATA);

		boolean result = filter.filter(entry.build());
		Assert.assertEquals(true, result);
	}
}
