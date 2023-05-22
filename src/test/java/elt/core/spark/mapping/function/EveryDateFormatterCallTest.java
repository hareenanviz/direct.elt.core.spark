package elt.core.spark.mapping.function;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.spark.mapping.function.EveryDateFormatterMappingFunction;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class EveryDateFormatterCallTest {

	private HashMap<String, Object> row;
	private LinkedHashMap<String, AnvizentDataType> newStructure;
	private String dateFormat;
	private EveryDateFormatterMappingFunction everyDateFormatter;

	@Before
	public void init() throws InvalidArgumentsException, InvalidRelationException, UnsupportedException {
		row = new HashMap<String, Object>();
		row.put("key1", new Date());
		row.put("key2", 1000L);
		row.put("key3", new Date());
		row.put("key4", "test abc");
		row.put("key5", new Date());

		newStructure = new LinkedHashMap<>();
		newStructure.put("key1", new AnvizentDataType(Date.class));
		newStructure.put("key2", new AnvizentDataType(Long.class));
		newStructure.put("key3", new AnvizentDataType(Date.class));
		newStructure.put("key4", new AnvizentDataType(String.class));
		newStructure.put("key5", new AnvizentDataType(Date.class));

		// CASE 1:
		dateFormat = "yyyy-MM-dd HH:mm:ss";
		everyDateFormatter = new EveryDateFormatterMappingFunction(new ConfigBean(), new MappingConfigBean() {
			private static final long serialVersionUID = 1L;
		}, dateFormat, new LinkedHashMap<>(), newStructure, null, null);

		// CASE 2:
		dateFormat = "dd/MM/yyyy HH:mm:ss";
		everyDateFormatter = new EveryDateFormatterMappingFunction(new ConfigBean(), new MappingConfigBean() {
			private static final long serialVersionUID = 1L;
		}, dateFormat, new LinkedHashMap<>(), newStructure, null, null);
	}

	@Test
	public void everyDateFormatterCallTest() throws Exception {
		System.out.println(everyDateFormatter.call(row));
		Assert.assertTrue(true);
	}
}
