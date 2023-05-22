package elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.mapping.config.bean.ReplicateConfigBean;
import com.anvizent.elt.core.spark.mapping.function.ReplicateMappingFunction;

/**
 * @author Hareen Bejjanki
 *
 */
public class ReplicateCallTest {

	private LinkedHashMap<String, Object> row;
	private ArrayList<String> fields;
	private ArrayList<String> toFields;
	private ArrayList<Integer> toPositions;
	private ReplicateMappingFunction replicateFunction;

	@Before
	public void init() throws ImproperValidationException, InvalidConfigException, InvalidArgumentsException, InvalidRelationException {

		row = new LinkedHashMap<String, Object>();
		row.put("a", "value1");
		row.put("b", "value2");
		row.put("c", "value3");
		row.put("d", "value4");
		row.put("e", "value5");

		// CASE 1:
		fields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("b");
				add("c");
				add("c");
				add("d");
				add("d");
			}
		};
		toFields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("b0");
				add("c0");
				add("c1");
				add("d0");
				add("d1");
			}
		};

		toPositions = new ArrayList<Integer>() {
			private static final long serialVersionUID = 1L;
			{
				add(1);
				add(2);
				add(3);
				add(5);
				add(0);
			}
		};

		// CASE 2:
		fields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("b");
				add("c");
				add("c");
				add("d");
				add("d");
				add("e");
			}
		};
		toFields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("b0");
				add("c0");
				add("c1");
				add("d0");
				add("d1");
				add("e0");
			}
		};
		toPositions = new ArrayList<Integer>() {
			private static final long serialVersionUID = 1L;
			{
				add(1);
				add(2);
				add(3);
				add(5);
				add(4);
				add(0);
			}
		};

		// CASE 3:
		fields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("a");
				add("b");
				add("c");
				add("d");
				add("e");
			}
		};
		toFields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("a0");
				add("b0");
				add("c0");
				add("d0");
				add("e0");
			}
		};

		toPositions = new ArrayList<Integer>() {
			private static final long serialVersionUID = 1L;
			{
				add(1);
				add(2);
				add(5);
				add(4);
				add(3);
			}
		};

		replicateFunction = new ReplicateMappingFunction(null, new ReplicateConfigBean(fields, toFields, toPositions), new LinkedHashMap<>(),
		        new LinkedHashMap<>(), null, null, null);
	}

	@Test
	public void replicateCallTest() throws Exception {
		System.out.println(replicateFunction.call(row));
		Assert.assertTrue(true);
	}
}
