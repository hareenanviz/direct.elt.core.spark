package elt.core.spark.function;

import java.util.HashMap;

import org.junit.Test;

import junit.framework.Assert;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class AnvizentFunctionMappingPositiveCallTest extends AnvizentFunctionMappingCallTest {

	protected HashMap<String, Object> expectedValues;

	@Override
	public void initExpected() {
		expectedValues = getExpectedValues();
	}

	@Test
	public void test() throws Throwable {
		HashMap<String, Object> actualValues = function.process(sourceValues);
		System.out.println(actualValues);
		System.out.println(expectedValues);

		Assert.assertEquals(actualValues, expectedValues);
	}

	public abstract HashMap<String, Object> getExpectedValues();
}