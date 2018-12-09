package test;

import java.util.concurrent.ConcurrentHashMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import Interfaces.CompletionCallback;
import Interfaces.ExternalClientInterface;
import Interfaces.ExternalStorageInterface;
import crypto.CryptoProvider;
import evaluation.PerformanceEvaluationLogger;
import services.PutRequest;
import services.Request;
import services.Request.RequestType;
import services.RequestSource;
import services.ScheduledRequest;
import utils.ClientParameters;
import utils.Errors;
import utils.Log;
import utils.SessionState;
import utils.SystemParameters;

public class ClientTester implements CompletionCallback{
	protected Log log = Log.getInstance();
	protected SystemParameters sysParams = SystemParameters.getInstance();
	protected ClientParameters clientParams = ClientParameters.getInstance();
	
	protected CryptoProvider cp = CryptoProvider.getInstance();

	protected ExternalClientInterface client = null;
	protected ExternalStorageInterface storage = null;
	
	protected ConcurrentHashMap<String, ConcurrentNavigableMap<Long, String>> map 
					= new ConcurrentHashMap<String, ConcurrentNavigableMap<Long, String>>();
	
	protected volatile boolean failed = false;
	
	public ClientTester(ExternalClientInterface c, ExternalStorageInterface s)
	{
		client = c;
		storage = s;
		failed = false;
	}
	
	public int runTest(RequestSource reqSource, int length, SessionState ss)
	{
		assert(client != null && storage != null);
		 
		File stateFile = new File(sysParams.tempDirectoryFP + "/" + client.getName() + ".test.state");
		File testerStateFile = new File(sysParams.tempDirectoryFP + "/" + client.getName() + ".tester.state");
		
		if(ss.shouldReset() == false && testerStateFile.exists() == true)
		{
			try
			{
				FileInputStream fis = new FileInputStream(testerStateFile);
				ObjectInputStream is = new ObjectInputStream(fis);
				
				load(is);
				
				is.close();
			}
			catch(Exception e) { throw new RuntimeException(e); }
		}
		
		client.open(storage, stateFile, ss.shouldReset());  // open the client
		
		int reqsProcessed = 0;
		while(reqSource.hasNext() == true && failed == false && reqsProcessed < length)
		{
			Request req = reqSource.next();
			
			long reqId = req.getId();
			String key = req.getKey();
			RequestType type = req.getType();
			
			if(map.containsKey(key) == false) // add the key to the map
			{
				assert(type == RequestType.PUT);
				ConcurrentNavigableMap<Long, String> nm = new ConcurrentSkipListMap<Long, String>();
				map.put(key, nm);
			}
			
			ConcurrentNavigableMap<Long, String> nm = map.get(key);
			 // the map has to contain that key, or else we are getting a key for which there is no corresponding previous put
			assert(nm != null);
			
			if(type == RequestType.PUT)
			{			
				assert(nm.containsKey(reqId) == false);
				assert(nm.ceilingKey(reqId) == null);
				
				PutRequest put = (PutRequest)req;
				String hash = cp.getHexHash(put.getValue().getData());
				nm.put(reqId, hash);
				
				byte[] val = put.getValue().getData();
				int intVal = ByteBuffer.wrap(val).getInt();
				String msg = "(Tester) Put request " + reqId + ": " + key + " -> " + hash + " [" + intVal + ", " + val.length + "]";
				log.append(msg, Log.INFO);
			}
			else 
			{ 
				String msg = "(Tester) Get request " + reqId + ": " + key + " <-";
				log.append(msg, Log.INFO);
			}
			
			ScheduledRequest sreq = client.schedule(req, this);
			
			Request req2 = sreq.getRequest();
			assert(req2 != null);
			assert(req2.getId() == reqId);
			assert(req2.getKey() == key);
			assert(req2.getType() == type);
			
			reqsProcessed++;
		}		
		
		client.close(null); // close the client
		 
		if(failed == false) // if there were no error, save state, update session state
		{
			try
			{
				FileOutputStream fos = new FileOutputStream(testerStateFile);
				ObjectOutputStream os = new ObjectOutputStream(fos);
				
				save(os);
				
				os.flush();
				os.close();
			}
			catch(IOException e) { throw new RuntimeException(e); }
			
			ss.nextReqId += reqsProcessed;
		}
		File testLogFile = new File("./log/test-" + client.getName() + ".perf.log");
		PerformanceEvaluationLogger.getInstance().dumpToFile(testLogFile);
		
		return reqsProcessed;
	}

	@Override
	public void onSuccess(ScheduledRequest scheduled) 
	{
		assert(scheduled != null);
		assert(scheduled.isReady() == true);
		assert(scheduled.wasSuccessful() == true);
		
		Request req = scheduled.getRequest(); assert(req != null);
		
		String key = req.getKey(); assert(key != null);
		if(req.getType() == RequestType.PUT)
		{
			//assert(scheduled.getDataItem() instanceof EmptyDataItem);
		}
		else
		{
			byte[] val = scheduled.getDataItem().getData();
			if(val.length != clientParams.contentByteSize)
			{
				if(val.length > clientParams.contentByteSize) { Errors.error("Invalid PUT request data"); }
			
				// ensure val has the correct size
				val = Arrays.copyOf(val, clientParams.contentByteSize);
			}
			String hexHashOfValue = cp.getHexHash(val);
			assert(hexHashOfValue != null);
			
			assert(req.getType() == RequestType.GET);
			
			ConcurrentNavigableMap<Long, String> nm = map.get(key);
			Long correspondingPutReqId = nm.floorKey(req.getId());
			assert(correspondingPutReqId != null && correspondingPutReqId < req.getId());
			
			String expectedHexHash = nm.get(correspondingPutReqId.longValue());
			assert(expectedHexHash != null);
			
			if(expectedHexHash.equalsIgnoreCase(hexHashOfValue) == false)
			{
				int intVal = ByteBuffer.wrap(val).getInt();
				String msg = "(Tester) Get request " + scheduled.getRequest().getId() 
						+ " completed: " + key + " <- " + hexHashOfValue + "[" + intVal + ", " + val.length + "]" + " (expected: " + expectedHexHash 
						+ ", corresponding put: " + correspondingPutReqId + ")";
				log.append(msg, Log.ERROR);
				
				log.forceFlush();
				testFailed(); // the client is faulty
			}
		}
		
	}

	private void testFailed() { failed  = true; }
	
	public boolean hasFailed() { return failed; }

	@Override
	public void onFailure(ScheduledRequest scheduled) 
	{
		failed = true;
		assert(true == false); // Something went wrong... (Coding FAIL)!
	}
	
	protected void load(ObjectInputStream is) throws Exception
	{
		int size = is.readInt();
		
		for(int i = 0; i < size; i++)
		{
			String k = (String)is.readObject();
			long kl = is.readLong();
			String kk = (String)is.readObject();
			
			ConcurrentNavigableMap<Long, String> cnm = new ConcurrentSkipListMap<Long, String>();
			cnm.put(kl, kk);
			map.put(k, cnm);
		}
	}
	
	protected void save(ObjectOutputStream os) throws IOException
	{
		os.writeInt(map.size()); // first the size
		for(String k : map.keySet())
		{
			ConcurrentNavigableMap<Long, String> v = map.get(k);
			Map.Entry<Long, String> entry = v.lastEntry();
			
			os.writeObject(k);
			os.writeLong(entry.getKey());
			os.writeObject(entry.getValue());
		}
	}
}
