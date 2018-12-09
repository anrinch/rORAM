package client;

public class LockObject {

	private static LockObject lock = new LockObject();
	private LockObject(){}
	
	public static LockObject getInstance(){
		return lock;
	}
	
	
}
