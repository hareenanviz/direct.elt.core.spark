package com.anvizent.elt.core.spark.bean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Test {

	public static void main(String[] args) throws ParseException {
		SimpleDateFormat myDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		myDate.setTimeZone(TimeZone.getTimeZone("UTC"));
		String newDate = myDate.format(new Date());
		System.out.println(newDate);
		myDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		newDate = myDate.format(new Date());
		System.out.println(newDate);
	}

}
