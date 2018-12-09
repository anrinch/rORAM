package services;

import services.Service.ObjectType;
import services.Service.ServiceType;

public class OperationLogItem {
	protected long reqId = -1;
	protected long opId = -1;
	protected String key = null;
	protected ServiceType type = null;
	
	protected boolean failed = false;
	
	protected long scheduleTime = -1;
	protected long completeTime = -1;
	
	protected long objectByteSize = -1;
	
	public OperationLogItem(Service op) 
	{
		reqId = op.getRequestId();
		opId = op.getOperationId(); 
		key = op.getKey(); 
		type = op.getType();
		
		failed = false;
	}
	
	private long getCurrentTime() { return System.currentTimeMillis(); }
	
	public void scheduled() { scheduleTime = getCurrentTime(); }
	public void completed() { completeTime = getCurrentTime(); }
	
	public void setObjectByteSize(long byteSize) { objectByteSize = byteSize; }
	
	@Override
	public String toString() 
	{
		double objectKB = objectByteSize / 1024.0;
		double elapsedTime = (completeTime - scheduleTime);
		
		String ret = "Op(" + opId + ", " + type.toString() + ", " + key + ", " + objectKB + "KB)";
		ret += " -> scheduled: " + scheduleTime + ", completed: " + completeTime + " (" + elapsedTime + " ms)";
		
		return ret;
	}

	public long getOperationId() { return opId;	}
	
	public void setFailed() { failed = true; }

	public double getObjectByteSize() { assert(objectByteSize >= 0); return objectByteSize;	}

	public ServiceType getType() { assert(type != null); return type; }
	
	public boolean isComplete() { return completeTime != -1; }

	public double elapsedTime() { return (completeTime - scheduleTime); }	
}
