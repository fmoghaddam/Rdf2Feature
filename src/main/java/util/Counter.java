package util;

/**
 * Atomic singleton counter
 * 
 * @author fbm
 *
 */
public class Counter {

	private static Counter instance = new Counter();

	private long counter = 0;

	// This private constructor is to prevent this object get instantiated more than
	// once.
	private Counter() {
	}

	public static Counter getInstance() {
		return instance;
	}

	public long getNext() {
		return ++this.counter;
	}

}