package runner;

import java.io.File;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



import crypto.CryptoProvider;
import Interfaces.CompletionCallback;
import Interfaces.ExternalClientInterface;
import Interfaces.ExternalStorageInterface;
import services.LogicalRequest;
import services.Request;
import services.ScheduledRequest;
import services.RequestSource;
import utils.*;
/**
 * Runs requests while tracking the performance at the application level.
 *
 */
public class TrackRequestsRunner extends ParallelRunner implements CompletionCallback
{	
	public class TagStats implements Comparable<TagStats>
	{
		protected LogicalRequest tag = null;
		protected int pendingReqs = 0;
		protected long scheduledTime = 0;
		protected long completedTime = 0;
		protected long logicalTraceTime = -1;
		protected long scheduledTraceTime = -1;
		protected long replayTraceTime = -1;
		
		@Override
		public int compareTo(TagStats o) { return (int)(scheduledTime - o.scheduledTime); }
	}
	
	protected Lock lock = new ReentrantLock();
	protected Map<LogicalRequest, TagStats> tagMap = new HashMap<LogicalRequest, TagStats>();
	protected List<TagStats> statsList = new ArrayList<TagStats>();
	
	protected boolean doTiming = false;
	protected long lastReqTime = -1;
	protected long lastScheduledReqTime = -1;
	
	protected long initialLogicalTraceTime = -1;
	protected long replayStartTime = -1;
	
	protected boolean countLogical = false;
	
	/*
	protected boolean doCaching = true;
	protected LRUCache cache = null;
	protected static final int cacheByteSize = 16 * 1024 * 1024;
	*/
	
	public TrackRequestsRunner(ExternalClientInterface c, ExternalStorageInterface s, boolean safe, boolean timing, boolean clogical) 
	{
		super(c, s, safe);
		/*if(doCaching == true)	{ cache = new LRUCache(cacheByteSize); }*/
		
		doTiming = timing;
		countLogical = clogical;
	}
	
	public TrackRequestsRunner(ExternalClientInterface c, ExternalStorageInterface s) { this(c, s, true, false, false); }
	
	/** Override open **/
	@Override
	public void open(File stateFile, RequestSource inputRS) 
	{ 
		if(inputRS != null)
		{
			if(ss.shouldReset() && ss.fastInit == true) 
			{ 
				
				// get the first requests and extract them to the session state, so the ORAM can used them for fast init
				Map<String, Request> map = new HashMap<String, Request>();
				while(inputRS.hasNext() == true)
				{
					Request req = inputRS.next();
					String key = req.getKey();

					Errors.verify(map.containsKey(key) == false);
					map.put(key, req);
				}
				// then put all previous requests where ORAM can use them (for fast init)
				ss.fastInitMap = map;
				
				log.append("[TRR (open)] populate fast-init map: " + map.size() + " entries", Log.INFO);
			}
		}
		
		// open here
		super.open(stateFile, null);
	}

	@Override
	public synchronized CompletionCallback onNew(Request req) 
	{
		CompletionCallback ret = super.onNew(req);
		Errors.verify(ret == null);
		
		LogicalRequest tag = req.getTag();
		boolean put = false;
		
		lock.lock(); // -----------------
		TagStats stats = tagMap.get(tag); 
		if(stats == null)
		{
			stats = new TagStats();			
			tagMap.put(tag, stats);
			put = true;
		}
		lock.unlock(); // ---------------
		
		if(put == true)
		{
			if(doTiming == true) 
			{ 
				long ts = req.getTimestamp(); Errors.verify(ts > 0);
					
				if(lastReqTime <= 0) { lastReqTime = ts;  }	
				long currentTime = System.currentTimeMillis();
				if(lastScheduledReqTime <= 0) { lastScheduledReqTime = currentTime; }
				//long elapsedSinceLast = currentTime - lastScheduledReqTime;
				//long interReqTime = ts - lastReqTime;
				//long delta = interReqTime - elapsedSinceLast;
				
				if(initialLogicalTraceTime < 0) { initialLogicalTraceTime = ts; replayStartTime = currentTime; }
				final long ltt = ts - initialLogicalTraceTime;
				
				final long elapsedSinceReplayStarted = (currentTime - replayStartTime);
				long delta = ltt - elapsedSinceReplayStarted;
					
				final long estimatedScheduleTime = 1; // it will take on the order of 1ms to schedule anyways.
				delta -= estimatedScheduleTime;
				
				if(delta > 0) { try { Thread.sleep(delta); } catch (InterruptedException e) { Errors.error(e); } }
				
				//long actualInterReqTime = elapsedSinceLast + (System.currentTimeMillis() - currentTime);
				long actualInterReqTime = (System.currentTimeMillis() - currentTime); // time between the previous schedule and this one
					
				lastReqTime = ts;
				
				/*stats.logicalTraceTime = interReqTime;
				stats.scheduledTraceTime = actualInterReqTime;*/

				stats.logicalTraceTime = ltt;
				stats.scheduledTraceTime = actualInterReqTime;
			}
			
			stats.tag = tag;
			stats.pendingReqs = tag.getReqsCount();
			stats.completedTime = -1;
			stats.scheduledTime = System.currentTimeMillis();
			if(doTiming == true) 
			{ 
				lastScheduledReqTime = stats.scheduledTime; 
				stats.replayTraceTime = stats.scheduledTime - replayStartTime;
			}
		}
				
		return this;
	}
	
	@Override
	public int run(RequestSource rs, int length)
	{
		if(countLogical == false) { return super.run(rs, length); }
		
		int processed = 0;
		
		int currentReqs = 0; LogicalRequest prevTag = null;
		while(rs.hasNext() == true && processed < length)
		{
			Request req = rs.next();
			LogicalRequest tag = req.getTag();
			
			Errors.verify(currentReqs == 0 || prevTag == tag); 
			
			CompletionCallback callback = onNew(req);
			
			ScheduledRequest sreq = client.schedule(req, callback);
			
			boolean success = onScheduled(sreq);
			
			if(success == false) { processFailure(sreq); }
			
			currentReqs++;
			if(currentReqs == tag.getReqsCount())
			{
				currentReqs = 0;
				processed++;
			}
			
			ss.nextReqId++; // increment all the time, so we seek the correct (non-logical) request when resuming
			
			prevTag = tag;
		}
		
		return processed;
	}
	
	@Override
	public synchronized boolean onScheduled(ScheduledRequest sreq) 
	{
		boolean ret = super.onScheduled(sreq);
		
		return ret; 
	}

	@Override
	public void onSuccess(ScheduledRequest sreq) 
	{
		long timeOfCall = System.currentTimeMillis();
		Request req = sreq.getRequest();
		LogicalRequest tag = req.getTag();
		
		lock.lock(); // -----------------
		
		TagStats stats = tagMap.get(tag);
		Errors.verify(stats != null && stats.tag == tag);
		
		stats.pendingReqs--;
		if(stats.pendingReqs == 0) 
		{ 
			stats.completedTime = timeOfCall; 
			tagMap.remove(tag);
			statsList.add(stats);
		}
		
		/*if(doCaching == true)
		{
			String key = req.getKey();
			boolean update = cache.containsKey(key);
			if(update == true) { cache.remove(key); }
			int bsz = sreq.getDataItem().getData().length;
			cache.put(key, bsz);	
		}*/
		
		lock.unlock(); // ---------------
	}

	@Override
	public void onFailure(ScheduledRequest sreq) 
	{
		; // do nothing, error will be handled elsewhere		
	}
	
	public List<Entry<String, List<Number>>> getStats() 
	{
		List<Entry<String, List<Number>>> ret = new ArrayList<>();
		Collections.sort(statsList);
		for(TagStats ts : statsList)
		{
			String name = ts.tag.getName();
			long elapsed = ts.completedTime - ts.scheduledTime;
			Errors.verify(elapsed >= 0);
			
			List<Number> l = new ArrayList<>();
			l.add(ts.scheduledTime); l.add(ts.completedTime);
			l.add(ts.logicalTraceTime); l.add(ts.scheduledTraceTime); l.add(ts.replayTraceTime);
			
			l.add(ts.tag.getByteSize()); l.add(ts.tag.getReqsCount());
			l.add(ts.tag.getRatio());
			
			ret.add(new AbstractMap.SimpleEntry<String, List<Number>>(name, l));
		}
		return ret; 
	}
}
