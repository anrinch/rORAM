package services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import services.Request.RequestType;
import services.Service.ServiceType;
import utils.Errors;

public class RequestLogItem {
	protected boolean init = false;
	protected long reqId = -1;
	protected String key = null;
	protected RequestType type = null;
	
	protected boolean failed = false;
	
	protected long scheduleTime = -1;
	protected long completeTime = -1;
	
	protected long itemByteSize = -1;
	
	protected Map<Long, OperationLogItem> operationsMap = new ConcurrentHashMap<Long, OperationLogItem>();
	
	public RequestLogItem(Request req) 
	{
		reqId = req.getId();
		key = req.getKey(); 
		type = req.getType();
		init = false;
		
		failed = false;
	}
	
	public RequestLogItem() // init request
	{
		reqId = Request.initReqId; 
		key = ""; type = null;
		init = true;
		
		failed = false;
	}
	
	private long getCurrentTime() { return System.currentTimeMillis(); }
	
	public void scheduled() { scheduleTime = getCurrentTime(); }
	public void completed() { completeTime = getCurrentTime(); }
	
	public void setItemByteSize(long byteSize) { itemByteSize = byteSize; Errors.verify(itemByteSize> 0); }
	
	@Override
	public String toString()
	{
		return toString(true);
	}
	
	public String toString(boolean summarizeOps) 
	{
		double elapsedTime = (completeTime - scheduleTime);
		
		String ret = "";
		
		if(failed == true) { ret += "<FAILED> "; }
		
		if(init == true) { ret += "InitReq"; }
		else 
		{
			double itemKB = itemByteSize / 1024.0;
			ret += "Req(" + reqId + ", " + type.toString() + ", " + key + ", " + String.format("%.2f", itemKB) + "KB)"; 
		}
		ret += " -> scheduled: " + scheduleTime + ", completed: " + completeTime + " (" + elapsedTime + " ms)";
	
		if(operationsMap.size() > 0) { ret += "\n"; }
		
		if(summarizeOps == true)
		{
			ret +="\t [";
			
			if(operationsMap.size() > 0)
			{
				// summarize ops
				double averageTime = 0.0; 
				int total = operationsMap.size(); int completed = 0; 
				long numD = 0; long numU = 0; long numRM = 0; long numC = 0; long numL = 0;
				double downloadedKB = 0.0; double uploadedKB = 0.0;
				for(long opId : operationsMap.keySet())
				{
					OperationLogItem opli = operationsMap.get(opId);
					ServiceType opType = opli.getType();
					
					if(opli.isComplete() == true)
					{
						completed++;
						switch(opType)
						{
						case DOWNLOAD: numD++; downloadedKB += opli.getObjectByteSize(); break;
						case UPLOAD: numU++; uploadedKB += opli.getObjectByteSize();  break;
						case DELETE: numRM++; break;
				
						}
					
						averageTime += opli.elapsedTime();
					}
				}
				averageTime /= completed;
				
				downloadedKB /= 1024.0; uploadedKB /= 1024.0;
				
				ret += total + " ops (" + (total - completed) + " failed, " + numD + " D, " + numU + " U, " + numRM + " RM, "  + numC + " C, " + numL + "L): ";
				ret += String.format("%.1f", averageTime) + " ms/op (" + String.format("%.1f", elapsedTime/completed);
				ret += " seq ms/op) <" + String.format("%.1f", downloadedKB) + "KB DL, " + String.format("%.1f", uploadedKB) + "KB UL>";
			}
			ret += "]";
		}
		else
		{
			int i = 0;
			for(long opId : new TreeSet<Long>(operationsMap.keySet()))
			{
				if(i > 0) { ret += "\n"; }
				
				ret +="\t";
				
				OperationLogItem opli = operationsMap.get(opId);
				
				ret += "[" + opli.toString() + "]";
			
				i++;
			}
		}
		
		return ret;
	}

	public void AddOperation(OperationLogItem opLogItem) 
	{
		long opId = opLogItem.getOperationId();
		
		//if(log.shouldLog(Log.INFO) == true) { log.append("[RLI] AddOperation(" + opId + ")", Log.INFO); }
		
		OperationLogItem tmp = operationsMap.put(opId, opLogItem);
		if(tmp != null)	{ Errors.error("Operation " + opId + " has already been added!"); }
	}
	
	public OperationLogItem LookupOperation(long opId) 
	{
		//if(log.shouldLog(Log.INFO) == true) { log.append("[RLI] LookupOperation(" + opId + ")", Log.INFO); }
		
		OperationLogItem ret = operationsMap.get(opId);
		if(ret == null) { Errors.error("Operation " + opId + " was not found!"); }
		
		return ret;
	}

	public void setFailed() { failed = true; }

	public boolean isInit() { return init; }
	public boolean hasFailed() { return failed; }
	
	public double getItemKB() { assert(itemByteSize > 0); return itemByteSize / 1024.0; }
	public double getElapsedTime() { return (completeTime - scheduleTime) / 1000.0; }
	public int getOpsCount() { return operationsMap.size(); }
	
	public boolean isComplete() { return completeTime != -1; }

	public double getOpsCompleteCount() 
	{
		double ret = 0.0;
		for(OperationLogItem logItem : operationsMap.values()) { if(logItem.isComplete() == true) { ret += 1.0; } }
		return ret;
	}

	public int getOpsCount(ServiceType type) 
	{
		int ret = 0;
		for(OperationLogItem logItem : operationsMap.values()) { if(logItem.getType() == type) { ret++; } }
		return ret;
	}
	
	public double getOpsKB(ServiceType type) 
	{
		double ret = 0.0;
		for(OperationLogItem logItem : operationsMap.values()) { if(logItem.getType() == type) { ret += logItem.getObjectByteSize(); } }
		return ret / 1024.0;
	}
	
	public List<Double> getOpsTimes(ServiceType type)
	{
		List<Double> ret = new ArrayList<Double>();
		for(OperationLogItem logItem : operationsMap.values()) { if(logItem.getType() == type) { ret.add(logItem.elapsedTime()); } }
		return ret;
	}
}
