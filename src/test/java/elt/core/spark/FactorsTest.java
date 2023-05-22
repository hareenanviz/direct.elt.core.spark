package elt.core.spark;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FactorsTest {

	public static void main(String[] args) {
		int count = 0;

		for (int i = 1; i < 100000; i++) {
			if (isPrime(i) && (("" + i).endsWith("7"))) {
				System.out.println(i);
				count++;
			}
		}

		System.out.println(count);
		
		System.out.println(Math.sqrt(100000d));
	}

	private static boolean isPrime(int i) {
		return numberOfFactors(i) == 0;
	}

	private static int numberOfFactors(int n) {
		int count = 0;

		int i = 2;

		for (; i * i < n; i++) {
			if (n % i == 0) {
				count += 2;
			}
		}

		if (i * i == n) {
			count++;
		}

		return count;
	}

}
