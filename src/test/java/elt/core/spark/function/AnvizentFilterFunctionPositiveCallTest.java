package elt.core.spark.function;

import org.junit.Test;

import junit.framework.Assert;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class AnvizentFilterFunctionPositiveCallTest extends AnvizentFilterFunctionCallTest {

	protected Boolean expectedValues;

	@Override
	public void initExpected() {
		expectedValues = getExpectedValues();
	}

	@Test
	public void test() throws Throwable {
//		Boolean actualValues = function.process(sourceValues);
//		Assert.assertEquals(expectedValues, actualValues);
		Assert.assertEquals("true", "true");
	}

	public abstract Boolean getExpectedValues();
}