package elt.core.spark.mapping.function;

import java.util.Date;
import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.spark.constant.AppendAt;
import com.anvizent.elt.core.spark.mapping.config.bean.DuplicateConfigBean;
import com.anvizent.elt.core.spark.mapping.function.DuplicateMappingFunction;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DuplicateCallTest {

	private LinkedHashMap<String, Object> row;
	private DuplicateMappingFunction duplicate;
	private String prefix;
	private String suffix;
	private AppendAt appendAt;

	@Before
	public void init() throws InvalidArgumentsException, InvalidRelationException {
		row = new LinkedHashMap<String, Object>();
		row.put("key1", new Date());
		row.put("Discount_Percent", 1000L);
		row.put("key3", new Date());
		row.put("key4", "test abc");
		row.put("key5", 5.2655f);
		row.put("key6", 105.5);
		row.put("key7", 500);
		row.put("key8", (byte) 10);
		row.put("key9", (short) 454);
		row.put("key10", 'A');

		prefix = "string_of_";
		suffix = null;
		appendAt = AppendAt.NEXT;

		duplicate = new DuplicateMappingFunction(null, new DuplicateConfigBean(true, prefix, suffix, appendAt), new LinkedHashMap<>(), new LinkedHashMap<>(),
				null, null, null);
	}

	@Test
	public void duplicateCallTest() throws Exception {
		System.out.println(duplicate.call(row));
		Assert.assertTrue(true);
	}
}
