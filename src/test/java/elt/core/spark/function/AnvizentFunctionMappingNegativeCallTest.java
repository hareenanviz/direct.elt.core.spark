package elt.core.spark.function;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class AnvizentFunctionMappingNegativeCallTest extends AnvizentFunctionMappingCallTest {

	protected Class<? extends Throwable> expectedException;

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Override
	public void initExpected() {
		expectedException = getExpectedException();
	}

	@Test
	public void test() throws Throwable {
		exception.expect(expectedException);
		try {
			function.process(sourceValues);
		} catch (Throwable exp) {
			for (int i = 0; i < getCauseDepth(); i++) {
				exp = exp.getCause();
			}
			throw exp;
		}
	}

	public abstract Class<? extends Throwable> getExpectedException();

	public abstract int getCauseDepth();
}
