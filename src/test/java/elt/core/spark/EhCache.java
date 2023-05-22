package elt.core.spark;

import java.util.LinkedHashMap;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class EhCache {

	public static void main(String[] args) throws InterruptedException {

		LinkedHashMap<String, Object> row1 = getRow();
		
		LinkedHashMap<String, Object> row2 = getRow();
		
		System.out.println(row1.equals(row2));
		
		System.out.println(row1.hashCode()+"  "+row2.hashCode());
		
		CacheManager cm = CacheManager.getInstance();
		
		Cache cache = new Cache("cache1", 100, true, false, 8, 5, false, 100);
		cm.addCache(cache);

		cache.put(new Element("1", row1));
		cache.put(new Element("2", row2));

		Element e = cache.get("2");
		String output = e.getObjectValue().toString();
		System.out.println(output);

		System.out.println(cache.get("1"));
		System.out.println(cache.get("2"));

		Thread.sleep(1000 * 3);
		
		
		System.out.println(cache.get("1"));
		//System.out.println(cache.get("2"));
		

		Thread.sleep(1000 * 3);
		System.out.println(cache.get("1"));
		System.out.println(cache.get("2"));
		
		
		Thread.sleep(1000 * 3);
		System.out.println(cache.get("1"));
		System.out.println(cache.get("2"));
		
		cm.shutdown();
	}

	private static LinkedHashMap<String, Object> getRow() {
		LinkedHashMap<String, Object> row1 = new LinkedHashMap<>();
		row1.put("1", "abc");
		row1.put("2", "def");
		row1.put("3", "ghi");
		return row1;
	}

}
