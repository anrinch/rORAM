package services;

import java.util.concurrent.atomic.AtomicLong;


public abstract class Service {
	
	public enum ServiceType {DOWNLOAD, UPLOAD, DELETE, COPY}
	public enum ObjectType {Bucket,Block,Metadata}
	private static AtomicLong nextOpId = new AtomicLong(1);
	
	protected long reqId = 0;
	protected long opId = 0;
	
	protected String key = null;
	
	public Service(long r, String k)
	{
		opId = nextOpId.getAndIncrement();
		reqId = r;
		key = k;
	}
	
	public String getKey() { return key; }
	public long getRequestId() { return reqId; }
	public long getOperationId() { return opId; }
	
	public abstract ServiceType getType();
	public abstract ObjectType getObjectType();
	public abstract void setObjectType(ObjectType obtype);
}
