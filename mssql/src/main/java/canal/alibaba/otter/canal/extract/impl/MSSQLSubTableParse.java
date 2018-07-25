package canal.alibaba.otter.canal.extract.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import canal.alibaba.otter.canal.extract.AbstractMSSqlParse;

/**
 * 通过主表的实例传过来的订单id去关联查询数据，然后发送kafka
 * 
 * @author user
 *
 */

// @Configuration
// @PropertySource("classpath:databases/200.properties")
// @ConfigurationProperties(prefix = "canal.instance")
public class MSSQLSubTableParse extends AbstractMSSqlParse {

	private final static Logger logger = LoggerFactory.getLogger(MSSQLSubTableParse.class);
	private String subTableList;
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Override
	public void parse() {

	}

	/**
	 * 
	 */
	public void extractSubTableData(Collection<Integer> ids) {
		Set set = new HashSet();
		set.add(273166410);
		set.add(273166401);
		set.add(273166404);
		set.add(268054519);
		set.add(272244776);
		set.add(273170526);
		set.add(273170516);
		set.add(273047643);
		set.add(272117798);
		set.add(273035352);

		MapSqlParameterSource parameters = new MapSqlParameterSource();
		parameters.addValue("ids", set);
		List list1 = namedParameterJdbcTemplate.queryForList("select  * from mobileapiorderfrom  WITH(NOLOCK) where orderId in (:ids) ", parameters);
		for (int i = 0; i < list1.size(); i++) {
			System.out.println(i + "   " + list1.get(i));
		}
	}

	public void stop() {
		super.stop();
	}

	public String getSubTableList() {
		return subTableList;
	}

	public void setSubTableList(String subTableList) {
		this.subTableList = subTableList;
	}

	public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
		return namedParameterJdbcTemplate;
	}

	public void setNamedParameterJdbcTemplate(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}

}