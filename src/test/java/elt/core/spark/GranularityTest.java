package elt.core.spark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class GranularityTest {

	public static void main(String[] args) throws ParseException {
		String sampleDate = "2015-05-20 15:56:46.200";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date pastDate = sdf.parse(sampleDate);

		System.out.println(new java.sql.Timestamp(pastDate.getTime() - (pastDate.getTime() % (60 * 60 * 1000))));

		System.out.println(
				"reduce this time - " + new java.sql.Timestamp(pastDate.getTime() % (60 * 60 * 1000)) + " , " + (pastDate.getTime() % (60 * 60 * 1000)));
		System.out.println("time from 1606200 seconds - " + new java.sql.Time(1606200));

		System.out.println("actual seconds to reduce - " + (56 * 46 * 1000 * (200f / 1000f)) + " , " + (new java.sql.Time((56 * 46 * 1000 * (200 / 1000)))));

		// System.out.println(new java.sql.Timestamp(pastDate.getTime() - (60 *
		// 60 * 1000)));

		/*
		 * long difference = new Date().getTime() - pastDate.getTime();
		 * System.out.println("Seconds - " + difference % 60);
		 * System.out.println("Minutes - " + difference / 60 % 60);
		 * System.out.println("Hours - " + difference / 3600 % 24);
		 * System.out.println("Days - " + difference / 86400 % 7);
		 * System.out.println("Week - " + difference / 86400 / 7);
		 */

		/*
		 * Calendar calendar = Calendar.getInstance();
		 * 
		 * calendar.setTime(now);
		 * 
		 * calendar.set(Calendar.MILLISECOND, 0); calendar.set(Calendar.SECOND,
		 * 0); calendar.set(Calendar.MINUTE, 0);
		 * calendar.set(Calendar.HOUR_OF_DAY, 0);
		 * calendar.set(Calendar.DAY_OF_MONTH, 1); calendar.set(Calendar.MONTH,
		 * 0);
		 * 
		 * System.out.println(new
		 * java.sql.Timestamp(calendar.getTimeInMillis()));
		 */
	}
}
