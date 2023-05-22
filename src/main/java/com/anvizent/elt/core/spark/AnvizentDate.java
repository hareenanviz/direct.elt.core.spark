package com.anvizent.elt.core.spark;

import java.util.TimeZone;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class AnvizentDate {

	private long time;
	private TimeZone timeZone;

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public TimeZone getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(TimeZone timeZone) {
		this.timeZone = timeZone;
	}

	public void changeTimeZone(TimeZone timeZone) {
		long timeDifference = this.timeZone.getRawOffset() - timeZone.getRawOffset() + this.timeZone.getDSTSavings() - timeZone.getDSTSavings();

		time -= timeDifference;

		this.timeZone = timeZone;
	}

}
