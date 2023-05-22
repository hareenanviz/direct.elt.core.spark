package elt.core.spark;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class JodaDateWithTimeZone {

	public static void main(String[] args) throws ParseException {
		String pattern = "yyyy-MM-dd HH:mm:ss";

		String sampleDate = "2017-08-14 04:47:00";
		String newTimeZoneDate = "2017-08-14 18:47:00";

		DateTimeZone timeZone1 = DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Cuiaba"));
		DateTimeZone timeZone2 = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Australia/Sydney"));

		test1(sampleDate, pattern, timeZone2);
		test2(sampleDate, newTimeZoneDate, pattern, timeZone1, timeZone2);

		Date date = new Date(new SimpleDateFormat(pattern).parse(sampleDate).getTime());
		Date newTimeZoneSQLDate = new Date(new SimpleDateFormat(pattern).parse(newTimeZoneDate).getTime());

		test1(date, pattern, timeZone2);
		test2(date, newTimeZoneSQLDate, pattern, timeZone1, timeZone2);
	}

	private static DateTime getJodaDateTime(String sampleDate, String pattern, DateTimeZone timeZone1) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern(pattern).withZone(timeZone1);
		return dtf.parseDateTime(sampleDate);
	}

	private static void test2(String sampleDate, String newTimeZoneDate, String pattern, DateTimeZone timeZone1, DateTimeZone timeZone2) throws ParseException {
		// TODO Auto-generated method stub

		DateTime jodaTime = getJodaDateTime(sampleDate, pattern, timeZone1);
		System.out.println("jodaTime " + jodaTime.toLocalDateTime());
		Timestamp dateFromDB1 = new Timestamp(new SimpleDateFormat(pattern).parse(sampleDate).getTime());
		Timestamp dateFromRow1 = new Timestamp(jodaTime.getMillis());
		System.out.println("dateFromDB1 " + dateFromDB1);
		System.out.println("dateFromRow1 " + dateFromRow1);
		System.out.println(dateFromDB1.equals(dateFromRow1));

		System.out.println("-----------------------------------------");

		DateTime jodaTimeNewTimeZone = new DateTime(jodaTime, timeZone2);
		System.out.println("jodaTimeNewTimeZone " + jodaTimeNewTimeZone.toLocalDateTime());
		Timestamp dateFromDB2 = new Timestamp(new SimpleDateFormat(pattern).parse(newTimeZoneDate).getTime());
		Timestamp dateFromRow2 = new Timestamp(jodaTimeNewTimeZone.getMillis());
		System.out.println("dateFromDB2 " + dateFromDB2);
		System.out.println("dateFromRow2 " + dateFromRow2);

		System.out.println(dateFromDB2.equals(dateFromRow2));

		System.out.println(dateFromDB2.getTime());
		System.out.println(dateFromRow2.getTime());

		System.out.println((dateFromDB2.getTime() - dateFromRow2.getTime()) / 3600000);
	}

	private static DateTime getDateTime(String sampleDate, String pattern, DateTimeZone timeZone1) {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
		DateTime jodaTime = formatter.parseDateTime(sampleDate);

		if (timeZone1 != null) {
			return new DateTime(jodaTime, timeZone1);
		} else {
			return jodaTime;
		}
	}

	private static void test1(String sampleDate, String pattern, DateTimeZone timeZone) {
		// TODO Auto-generated method stub
		DateTime jodaTime = getDateTime(sampleDate, pattern, null);
		System.out.println("joda datetime4 --- " + jodaTime.getMillis());
	}

	private static void test2(Date date, Date newTimeZoneSQLDate, String pattern, DateTimeZone timeZone1, DateTimeZone timeZone2) {
		// TODO Auto-generated method stub

	}

	private static void test1(Date date, String pattern, DateTimeZone timeZone2) {
		// TODO Auto-generated method stub

	}
}
