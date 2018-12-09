package client;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.DataItem;
import data.EmptyDataItem;
import data.Header;
import data.SimpleDataItem;
import pollable.Pollable;

import services.DownloadObject;
import services.GetRequest;
import services.PutRequest;
import services.Request;
import services.ScheduledOperation;
import services.ScheduledRequest;
import services.UploadObject;
import utils.Errors;
import utils.SessionState;

/**
 * Implements a baseline RAM client.
 * <p><p>
 * This client is not oblivious: it simply translate each request it receives into an
 * equivalent storage access.
 * 
 */
public class RAMClient extends AbstractClient 
{
	protected boolean synchronous = false;
	
	public RAMClient() { this(true); } // by default synchronous
	public RAMClient(boolean sync) { synchronous = sync; }
	
	protected String getObjectKey(String key) 
	{
		return getName() + "-Block--" + key;
	}
	
	@Override
	public boolean isSynchronous() { return synchronous; }

	@Override
	public String getName() { return "RAMClient"; }
	
	
	@Override
	protected void load(ObjectInputStream os)  throws Exception { ; } // nothing to load
	
	@Override
	protected void save(ObjectOutputStream os)  throws Exception { ; } // nothing to save
	
	@Override
	protected void init(boolean reset) 
	{
		SessionState ss = SessionState.getInstance();
		if(reset == true && ss.fastInit == true)
		{
			Map<String, Request> m = ss.fastInitMap;
			if(m != null)
			{
				long totalByteSize = 0;
				List<ScheduledOperation> pendingUploads = new ArrayList<ScheduledOperation>();
				for(String k : m.keySet())
				{
					Request req = m.get(k);
					PutRequest put = (PutRequest)req;
					
					//String objectKey = getObjectKey(req.getKey());
					String objectKey = "RAMClient";
					
					
					byte[] val = put.getValue().getData();
					if(val.length < clientParams.minContentByteSize) { val = Arrays.copyOf(val, clientParams.minContentByteSize); }
					
					totalByteSize += val.length;
					
					byte[] data =val;
					DataItem d = new SimpleDataItem(data);
					UploadObject op = new UploadObject(Request.initReqId, objectKey, d);
					op.setObjectOffset(clientParams.minContentByteSize * Integer.parseInt(req.getKey()));
					op.setObjectSize(clientParams.minContentByteSize);
					ScheduledOperation sop = s.uploadObject(op);
					
					pendingUploads.add(sop);
					
				}
				Pollable.waitForCompletion(pendingUploads);
				
				//{ log.append("[RAMClient] fast-init uploaded " + pendingUploads.size() + " items (" + (totalByteSize / (1024 * 1024)) + "MB).", Log.INFO); }
			}
		}
	}

	@Override
	public ScheduledRequest scheduleGet(GetRequest req) 
	{
		long reqId = req.getId();
		String key = req.getKey();
		
		String objectKey = "RAMClient";
		DownloadObject op = new DownloadObject(reqId, objectKey);
		
		ScheduledOperation sop = s.downloadObject(op);

		op.setObjectOffset(clientParams.minContentByteSize * Integer.parseInt(req.getKey()));
		op.setObjectSize(clientParams.minContentByteSize);
	
		ScheduledRequest sreq = new ScheduledRequest(req);
		
		if(synchronous == true)
		{
			sop.waitUntilReady();
			if(sop.wasSuccessful() == true) 
			{ 
				byte[] decrypted = sop.getDataItem().getData();
				sreq.onSuccess(new SimpleDataItem(decrypted)); 
			}
			else { sreq.onFailure(); }
		}
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{		
		long reqId = req.getId();
		String key = req.getKey();
		byte[] val = req.getValue().getData();
		if(val.length < clientParams.minContentByteSize) { val = Arrays.copyOf(val, clientParams.minContentByteSize); }
		
		//String objectKey = getObjectKey(key);
		
		String objectKey = "RAMClient";
		byte[] data = val;
		UploadObject op = new UploadObject(reqId, objectKey, new SimpleDataItem(data));
		op.setObjectOffset(clientParams.minContentByteSize * Integer.parseInt(req.getKey()));
		op.setObjectSize(clientParams.minContentByteSize);
	
		ScheduledOperation sop = s.uploadObject(op);

		ScheduledRequest sreq = new ScheduledRequest(req);
		
		if(synchronous == true)
		{
			sop.waitUntilReady();
			if(sop.wasSuccessful() == true) { sreq.onSuccess(sop.getDataItem()); }
			else { sreq.onFailure(); }
		}
	
		
		return sreq;
	}
	
	@Override
	public long peakByteSize() { return clientParams.contentByteSize; } // at most 1 block at a time
}
