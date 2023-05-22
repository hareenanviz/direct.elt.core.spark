package com.anvizent.elt.core.spark;

import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

public class Test {

	public static void main(String[] args) throws IOException {
		System.out.println("test1234".matches("test.*"));
		CSVFormat format = CSVFormat.RFC4180.withDelimiter(',').withQuote('"');
		CSVParser csvParser = CSVParser.parse("a,b,c", format);
		System.out.println(csvParser.getRecords().get(0).size());
		System.out.println(csvParser.getHeaderMap());
		System.out.println(Byte.valueOf("23345345345345345345.34"));
	}

}
