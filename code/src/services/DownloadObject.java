package services;
import data.*;

public class DownloadObject extends Service{

	private byte[] data = null;
	public DownloadObject(long r, String k) { super(r, k); }
	private ObjectType objectType;
	private int objectOffset;
	private int objectSize;
	
	protected DownloadObject(long r, long o, String k) // constructor (unsafe)
	{
		super(r, k);
		opId = o;
		objectType = ObjectType.Bucket; // Bucket, unless set explicitly
	}
	
	@Override
	public ServiceType getType() {return ServiceType.DOWNLOAD;}
	public ObjectType getObjectType(){ return objectType; }
	public void setObjectType(ObjectType obtype){objectType = obtype;}
	public void setObjectOffset(int offset){ this.objectOffset = offset;}
	public int getObjectOffset(){ return this.objectOffset;}
	public void setObjectSize(int size){ this.objectSize = size;}
	public int getObjectSize(){ return this.objectSize;}
	
	
	public void setData(byte[] d) {data = d;}

	
}
