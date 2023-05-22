package elt.core.spark;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.lang3.Range;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RangeTest {

	public static void main(String[] args) throws ParseException {
		Range<Byte> byteRange = Range.between((byte) 10, (byte) 127);
		System.out.println("Byte - " + byteRange.contains((byte) 127));

		Range<Short> shortRange = Range.between((short) 10, (short) 127);
		System.out.println("short - " + shortRange.contains((short) 129));

		Range<Character> charRange = Range.between((char) 10, (char) 127);
		System.out.println("char - " + charRange.contains((char) 10));

		Range<Integer> intRange = Range.between((int) 10, (int) 127);
		System.out.println("int - " + intRange.contains((int) 155));

		Range<String> stringRange = Range.between("abc", "def");
		System.out.println("int - " + stringRange.contains("eaa"));

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String sdate1 = "2017-10-25 11:59:00";
		Date date1 = sdf.parse(sdate1);

		String sdate2 = "2017-10-25 11:59:00";
		Date date2 = sdf.parse(sdate2);

		String sdate3 = "2017-10-27 24:59:00";
		Date date3 = sdf.parse(sdate3);

		Range<Date> dateRange = Range.between(date2, date3);
		System.out.println("date - " + dateRange.contains(date1));

		Range<BigDecimal> decimalRange = Range.between(new BigDecimal(15), new BigDecimal(100));
		System.out.println("date - " + decimalRange.contains(new BigDecimal(99)));

		System.out.println("regex -- " + Pattern.matches("[a-zA-Z0-9]{1,}", "adSSASSsadrun35454"));

		System.out.println("regex -- " + Pattern.matches("[\\d]{1,}", "123458958"));

		System.out.println("regex -- " + Pattern.matches("[0-9]{1,}[.][985]{3}", "5.2553145589E7"));

		System.out.println("regex -- " + Pattern.matches("[10]{2}[.][0-9]{1}", "10.2"));

		System.out.println("regex -- " + Pattern.matches("[a-zA-Z0-9.]+@[a-zA-Z0-9]+.[a-zA-Z]{2,3}", "apurva09@aas.com"));

	}
}
