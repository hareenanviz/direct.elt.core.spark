package elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.mapping.config.bean.ConstantConfigBean;
import com.anvizent.elt.core.spark.mapping.function.ConstantMappingFunction;

/**
 * @author Hareen Bejjanki
 *
 */
public class ConstantCallTest {

	private LinkedHashMap<String, Object> row;
	private ArrayList<String> fields;
	private ArrayList<Class<?>> types;
	private ArrayList<String> formats;
	private ArrayList<String> values;
	private ArrayList<Integer> positions;
	private ConstantMappingFunction constant;

	@Before
	public void init() throws ImproperValidationException, InvalidConfigException, InvalidArgumentsException, InvalidRelationException {
		row = new LinkedHashMap<String, Object>();
		row.put("key1", "abc");
		row.put("key2", 12);
		row.put("key3", 12.5f);

		fields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;

			{
				add("const1");
				add("const2");
				add("const3");
			}
		};
		types = new ArrayList<Class<?>>() {
			private static final long serialVersionUID = 1L;

			{
				add(Float.class);
				add(Date.class);
				add(String.class);
			}
		};
		formats = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;

			{
				add("");
				add("yyyy-MM-dd HH:ss:mm");
				add("");
			}
		};
		values = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;

			{
				add("100");
				add("2017-07-20 12:13:01");
				add("abcd");
			}
		};

		positions = new ArrayList<Integer>() {
			private static final long serialVersionUID = 1L;

			{
				add(-3);
				add(4);
				add(-1);
			}
		};

		constant = new ConstantMappingFunction(null, new ConstantConfigBean(fields, types, formats, values, positions, null, null), new LinkedHashMap<>(),
		        new LinkedHashMap<>(), null, null, null);

	}

	@Test
	public void constantCallTest() throws Exception {
		System.out.println(constant.call(row));
		Assert.assertTrue(true);
	}
}
