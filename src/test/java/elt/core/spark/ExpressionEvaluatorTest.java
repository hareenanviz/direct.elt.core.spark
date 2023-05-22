package elt.core.spark;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExpressionEvaluatorTest {
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {

		String expression = "arg_0.compareTo(new java.math.BigDecimal(15.5)) == 0";

		String[] argumentFieldsByExpression = { "arg_0" };

		Class[] argumentTypesByExpression = { java.math.BigDecimal.class };

		Object[] expressionValues = { new BigDecimal(15.5) };

		try {
			ExpressionEvaluator expEvaluatior = new ExpressionEvaluator(expression, Boolean.class, argumentFieldsByExpression, argumentTypesByExpression);
			Object expEvaluatorObj = expEvaluatior.evaluate(expressionValues);
			System.out.println(expEvaluatorObj);

		} catch (CompileException | InvocationTargetException e) {
			e.printStackTrace();
		}
	}
}
