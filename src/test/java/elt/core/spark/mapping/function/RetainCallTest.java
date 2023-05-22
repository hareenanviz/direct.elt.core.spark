package elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;

import org.junit.Before;
import org.junit.Test;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.spark.mapping.config.bean.RetainConfigBean;
import com.anvizent.elt.core.spark.mapping.function.RetainMappingFunction;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RetainCallTest {

	private ArrayList<String> retainFields;
	private ArrayList<String> retainFieldsAs;
	private ArrayList<Integer> retainFieldsAt;
	private ArrayList<String> emitFields;
	private RetainMappingFunction retainFunction;
	private LinkedHashMap<String, Object> row;

	@Before
	public void init() throws ImproperValidationException, InvalidArgumentsException, InvalidRelationException {
		retainFields = new ArrayList<>();
		emitFields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("key1");
				add("key2");
				add("key5");
			}
		};

		retainFunction = new RetainMappingFunction(null, new RetainConfigBean(retainFields, retainFieldsAs, retainFieldsAt, emitFields), new LinkedHashMap<>(),
				null, null, null, null);

		row = new LinkedHashMap<>();
		row.put("key1", 10);
		row.put("key2", 20);
		row.put("key3", 30);
		row.put("key4", 10.52);
		row.put("key5", "abc");
		row.put("key6", new Date());
		row.put("key7", 1000);
		row.put("key8", 10f);
	}

	@Test
	public void retainCallTest() throws Exception {
		System.out.println(retainFunction.call(row));
	}

}
