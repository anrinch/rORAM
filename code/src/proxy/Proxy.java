package proxy;

import java.util.concurrent.locks.ReentrantLock;

public class Proxy {
	
	final ReentrantLock queryLock = new ReentrantLock();
	final ReentrantLock processingLock = new ReentrantLock();
	final ReentrantLock stashSetLock = new ReentrantLock();
}
