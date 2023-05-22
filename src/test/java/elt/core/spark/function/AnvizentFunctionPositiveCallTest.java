package elt.core.spark.function;

import java.util.HashMap;
import java.util.LinkedHashMap;

import org.junit.Test;

import junit.framework.Assert;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class AnvizentFunctionPositiveCallTest extends AnvizentFunctionCallTest {

	protected LinkedHashMap<String, Object> expectedValues;

	@Override
	public void initExpected() {
		expectedValues = getExpectedValues();
	}

	@Test
	public void test() throws Throwable {
		HashMap<String, Object> actualValues = function.process(sourceValues);
		Assert.assertEquals(expectedValues, actualValues);
	}

	public abstract LinkedHashMap<String, Object> getExpectedValues();
}