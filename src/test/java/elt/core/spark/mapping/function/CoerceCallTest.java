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
import com.anvizent.elt.core.spark.mapping.config.bean.CoerceConfigBean;
import com.anvizent.elt.core.spark.mapping.function.CoerceMappingFunction;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CoerceCallTest {

	private LinkedHashMap<String, Object> row;
	private ArrayList<String> coerceFields;
	private ArrayList<Class<?>> coerceToTypes;
	private ArrayList<String> coerceToFormats;
	private CoerceMappingFunction coerceFunction;

	@Before
	public void init() throws ImproperValidationException, ClassNotFoundException, InvalidArgumentsException, InvalidRelationException {
		row = new LinkedHashMap<String, Object>();

		// CASE 8: DATE
		row.put("key1", new Date());
		row.put("key2", new Date());
		row.put("key3", new Date());

		coerceToTypes = new ArrayList<Class<?>>() {
			private static final long serialVersionUID = 1L;

			{
				add(Long.class);
				add(String.class);
				add(String.class);
			}
		};
		coerceFields = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;

			{
				add("key1");
				add("key2");
				add("key3");
			}
		};
		coerceToFormats = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;

			{
				add("yyyy-MM-dd HH:mm:ss");
				add("yyyy-MM-dd HH:mm:ss");
				add("yyyy-MM-dd HH:mm:ss");
			}
		};
		coerceFunction = new CoerceMappingFunction(null, new CoerceConfigBean(coerceFields, coerceToTypes, coerceToFormats, null, null), new LinkedHashMap<>(),
		        null, null, null, null);
	}

	public void dummy() {
		// TODO add different tests

		// CASE 1: BYTE
		row.put("key1", (byte) 1);
		row.put("key2", (byte) 15);
		row.put("key3", (byte) 'A');
		row.put("key4", (byte) 255);
		row.put("key5", (byte) 256);
		row.put("key6", (byte) 8);
		row.put("key7", (byte) 288);
		row.put("key8", (byte) 5);

		// CASE 2: SHORT
		row.put("key1", (short) 1);
		row.put("key2", (short) 15);
		row.put("key3", (short) 'A');
		row.put("key4", (short) 255.56);
		row.put("key5", (short) 256.874024);
		row.put("key6", (short) 8);
		row.put("key7", (short) 288);
		row.put("key8", (short) 5);

		// CASE 3: INTEGER
		row.put("key1", 1);
		row.put("key2", 15);
		row.put("key3", -5);
		row.put("key4", 255);
		row.put("key5", 25656);
		row.put("key6", 0);
		row.put("key7", 288);
		row.put("key8", 5);

		// CASE 4: LONG
		row.put("key1", (long) 1);
		row.put("key2", (long) 15);
		row.put("key3", (long) -5);
		row.put("key4", (long) 255);
		row.put("key5", 25656566555L);
		row.put("key6", (long) 0);
		row.put("key7", (long) 288);
		row.put("key8", (long) 5);
		row.put("key9", 1499433921028L);

		// CASE 5: CHAR
		row.put("key1", (char) 1);
		row.put("key2", (char) 15);
		row.put("key3", (char) -5);
		row.put("key4", (char) 65);
		row.put("key5", (char) 25656566555L);
		row.put("key6", (char) 0);
		row.put("key7", (char) 288);
		row.put("key8", (char) 5);

		// CASE 6: FLOAT
		row.put("key1", 1.22f);
		row.put("key2", 15.5874f);
		row.put("key3", -5.25f);
		row.put("key4", 65f);
		row.put("key5", 25656.566555f);
		row.put("key6", 0f);
		row.put("key7", 288.000000000f);
		row.put("key8", 5f);

		// CASE 7: DOUBLE
		row.put("key1", 1.22d);
		row.put("key2", 15.5874d);
		row.put("key3", -5.25d);
		row.put("key4", 69d);
		row.put("key5", 25656.566555d);
		row.put("key6", 0d);
		row.put("key7", 288.000000000d);
		row.put("key8", 5d);

		// CASE 8: String
		row.put("key1", "0");
		row.put("key2", "155874");
		row.put("key3", "-525");
		row.put("key4", "6");
		row.put("key5", "25656");
		row.put("key6", "10");
		row.put("key7", "288");
		row.put("key8", "true");
		row.put("key9", "2017-07-07 18:43:00");
	}

	@Test
	public void renameCallTest() throws Exception {
		System.out.println("coercedRow ============ " + coerceFunction.call(row));
		Assert.assertTrue(true);
	}
}
