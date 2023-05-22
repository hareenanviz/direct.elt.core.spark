package elt.core.spark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class Test {

	public static void main(String[] args) {
		/*
		 * short s = '0' + '5';
		 * 
		 * System.out.println((int) '0'); System.out.println((int) '5');
		 */

		// byte conversion
		byte b = 12;
		String sb = Byte.toString(b); // byte to String
		char cb = (char) b; // byte to char
		double db = b;
		float fb = b;
		long lb = b;
		int ib = b;
		short shb = b;
		System.out.println("byte Conversion -- From --" + b + " -- To --> String - " + sb + " \t " + "long - " + lb + " \t " + "double - " + db + " \t "
				+ "float - " + fb + " \t " + "int - " + ib + " \t " + "short - " + shb + " \t " + "char - " + cb + " \t ");

		// char conversion
		String sch = String.valueOf(cb);
		double dch = cb;
		float fch = cb;
		long lch = cb;
		int ich = cb;
		short shch = (short) cb;
		byte bch = (byte) cb;
		System.out.println("char Conversion -- From --" + cb + " -- To --> String - " + sch + " \t " + "long - " + lch + " \t " + "double - " + dch + " \t "
				+ "float - " + fch + " \t " + "int - " + ich + " \t " + "short - " + shch + " \t " + "byte - " + bch + " \t ");

		// short conversion
		short sh = 125;
		String ssh = String.valueOf(sh);
		double dsh = sh;
		float fsh = sh;
		long lsh = sh;
		int ish = sh;
		char chsh = (char) sh;
		byte bsh = (byte) sh;
		System.out.println("short Conversion -- From --" + sh + " -- To --> String - " + ssh + " \t " + "long - " + lsh + " \t " + "double - " + dsh + " \t "
				+ "float - " + fsh + " \t " + "int - " + ish + " \t " + "char - " + chsh + " \t " + "byte - " + bsh + " \t ");

		// int conversion
		int i = 9;
		String si = String.valueOf(i); // or parse
		double di = i;
		float fi = i;
		long li = i;
		short shi = (short) i;
		char chi = (char) i;
		byte bi = (byte) i;
		System.out.println("int Conversion -- From --" + i + " -- To --> String - " + si + " \t " + "long - " + li + " \t " + "double - " + di + " \t "
				+ "float - " + fi + " \t " + "short - " + shi + " \t " + "char - " + chi + " \t " + "byte - " + bi + " \t ");

		// float conversion
		float f = 9.96f;
		String sf = String.valueOf(f);
		double df = f;
		int inf = (int) f;
		long lf = (long) f;
		short shf = (short) f;
		char chf = (char) f;
		byte bf = (byte) f;
		System.out.println("float Conversion -- From --" + f + " -- To --> String - " + sf + " \t " + "long - " + lf + " \t " + "double - " + df + " \t "
				+ "int - " + inf + " \t " + "short - " + shf + " \t " + "char - " + chf + " \t " + "byte - " + bf + " \t ");

		// long conversion
		long l = 12366547898L;
		String sl = String.valueOf(l);
		double dl = l;
		int il = (int) l;
		float fl = l;
		short shl = (short) l;
		char chl = (char) l;
		byte bytl = (byte) l;
		System.out.println("long Conversion -- From --" + l + " -- To --> String - " + sl + " \t " + "float - " + fl + " \t " + "double - " + dl + " \t "
				+ "int - " + il + " \t " + "short - " + shl + " \t " + "char - " + chl + " \t " + "byte - " + bytl + " \t ");

		// double conversion
		double d1 = 1.5565899d;
		String sd = String.valueOf(d1);
		float fd = (float) d1;
		long ld = (long) d1;
		int id = (int) d1;
		short shd = (short) d1;
		char cdd = (char) d1;
		byte bd = (byte) d1;
		System.out.println("Double Conversion -- From --" + d1 + " -- To --> String - " + sd + " \t " + "float - " + fd + " \t " + "long - " + ld + " \t "
				+ "int - " + id + " \t " + "short - " + shd + " \t " + "char - " + cdd + " \t " + "byte - " + bd + " \t ");

		// String conversion
		String sDate = "2015-05-05";
		SimpleDateFormat sfd = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date date = sfd.parse(sDate);
			System.out.println("String to date -- From --" + sDate + " -- To -- " + date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// boolean to String
		boolean booleanVal = true;
		String sBoolean = String.valueOf(booleanVal);
		System.out.println("boolean to String -- From --" + booleanVal + " -- To -- " + sBoolean);

		// String to double
		String s1 = "1225665489888";
		String s2 = "122dssd56";
		try {
			double ds1 = Double.parseDouble(s1);
			System.out.println("String to double -- From --" + s1 + " -- To -- " + ds1);
			double ds2 = Double.parseDouble(s2);
			System.out.println("String to double -- From --" + s2 + " -- To -- " + ds2);
		} catch (Exception e) {
			System.out.println("Conversion not supported.");
		}

	}

}
