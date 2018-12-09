package services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;

import utils.ClientParameters;
import utils.Log;
import data.DataItem;
import data.IntegerSeededDataItem;
import services.GetRequest;
import services.LogicalRequest;
import services.PutRequest;
import services.Request;
import services.Request.RequestType;
import utils.Errors;

/**
 * Implements a request source backed by an extracted trace.
 *
 */
public class ExtractedTraceRequestSource extends RequestSource 
{
	public static final int ExtractedTraceNumFields = 5;
	
	protected Log log = Log.getInstance();
	
	protected Random rng = new Random();
	protected StatefulObjectSplitter splitter = null;
	
	protected RequestSource inputRS = null;
	
	protected double goodputRatio = 0.0;
	protected long c = 0;
	
	public ExtractedTraceRequestSource(File traceFile, boolean split)
	{
		ClientParameters clientParams = ClientParameters.getInstance();
		int payloadSizePerBlock = clientParams.contentByteSize;
		splitter = new StatefulObjectSplitter(payloadSizePerBlock);
		
		TreeMap<Long, Request> tmpRequests = new TreeMap<Long, Request>();
		
		try
		{
			FileReader fr = new FileReader(traceFile);
			BufferedReader br = new BufferedReader(fr);
			
			String line = "";
			while((line = br.readLine()) != null) { parseLine(tmpRequests, line, split); c++; }
			
			br.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
		
		goodputRatio /= c;
		
		String msg = "[ETRS] " + "Goodput ratio: " + String.format("%.4f", goodputRatio) + " (for " + traceFile.getName() + ")";
		log.append(msg, Log.INFO);
		System.out.println(msg);
		
		splitter = null; // free the splitter
		
		filterReqs(tmpRequests);
		
		rewind();
	}

	private void filterReqs(TreeMap<Long, Request> map) 
	{
		inputRS = new RequestSource() {	}; 
		
		Set<String> keysSoFar = new HashSet<String>();
		long afterInitReqId = -1;
		for(long reqId : map.keySet())
		{
			Request req = map.get(reqId);
			RequestType type = req.getType();
			String key = req.getKey();
			
			boolean afterInitReq = (type == RequestType.GET || keysSoFar.contains(key) == true);
			
			if(afterInitReq == true) 
			{
				afterInitReqId = reqId;
				break;
			}
			else
			{
				inputRS.requests.put(reqId, req);
			}
			
			keysSoFar.add(key);
		}
		Errors.verify(afterInitReqId >= 0);
		
		requests.putAll(map.tailMap(afterInitReqId));
	}

	private void parseLine(Map<Long, Request> map, String line, boolean split) 
	{
		int payloadSize = ClientParameters.getInstance().contentByteSize;
		String s[] = line.split(",\\s+");
		
		Errors.verify(s.length == ExtractedTraceNumFields);
		
		String requestTime = s[0]; long ts = Long.parseLong(requestTime) * 1000;
		String requestType = s[1];
		String objectName = s[2];
		String bytesSent = s[3];
		String objectSize = s[4];
		
		Errors.verify(requestType.equalsIgnoreCase("GET") == true || requestType.equalsIgnoreCase("PUT") == true);
		RequestType type = requestType.equalsIgnoreCase("GET") == true ? RequestType.GET : RequestType.PUT;
		
		if(type == RequestType.PUT) { Errors.verify(bytesSent.equals("-")); }
		else { Errors.verify(bytesSent.equals(objectSize)); } // get
		
		int objSz = Integer.parseInt(objectSize);
		

		int bs = 0;
		if(type == RequestType.GET) { bs = Integer.parseInt(bytesSent); }
		
		int opSz = (bs > 0) ? bs : objSz;
		
		Errors.verify(objSz > 0 && opSz > 0);
		
		int offset = 0; // for now, we deal with cases where offset is 0
		
		List<Entry<String, Integer>> keys = null;
		if(split == true)
		{
			splitter.registerObject(objectName, objSz);
			keys = splitter.getKeys(objectName, offset, offset+opSz-1);
		}
		else
		{
			// just put the entire object
			keys = new ArrayList<Entry<String, Integer>>();
			objectName = objectName.replace('/', '_');
			keys.add(new AbstractMap.SimpleEntry<String, Integer>(objectName, objSz));
		}
		
		LogicalRequest tag = new LogicalRequest(objectName, objSz, keys.size());
		
		double ratio = 0.0;
		for(Entry<String, Integer> entry : keys)
		{
			String key = entry.getKey();
			int sz = entry.getValue();
			Errors.verify(sz > 0);
			
			ratio += split == true ? (double)sz/payloadSize : 1.0;
			
			Request req = null;
			switch(type)
			{
			case GET: { req = new GetRequest(key); } break;
			case PUT: 
				{
					DataItem di = new IntegerSeededDataItem(rng.nextInt(), sz);
					req = new PutRequest(key, di);
				}; break;
			default: Errors.error("Coding FAIL!");
			}
			
			Errors.verify(req != null);
			
			req.setTag(tag); // tag the request
			req.setTimestamp(ts); // set the timestamp
			
			map.put(req.getId(), req);
		}
		
		ratio /= keys.size();
		
		tag.setRatio(ratio);
		
		goodputRatio += ratio;
	}

	public RequestSource getInputRS() { inputRS.rewind(); return inputRS; }
}
