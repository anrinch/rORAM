package backend;
import Interfaces.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import evaluation.PerformanceEvaluationLogger;
import services.*;
import utils.*;

public class StorageAdapter implements ExternalStorageInterface{
	//protected PerformanceEvaluationLogger pe = PerformanceEvaluationLogger.getInstance();
	
	protected InternalStorageInterface storage = null;
	protected CompletionThread completionThread = null;
	
	protected boolean opened = false;
	
	public class CompletionThread extends Thread
	{
		protected volatile boolean done = false;		
		protected BlockingQueue<ScheduledOperation> queue = null;
		
		public CompletionThread(BlockingQueue<ScheduledOperation> q) { queue = q; }
		
		public void run()
		{
			Set<ScheduledOperation> pending = new HashSet<ScheduledOperation>();
			
			while(done == false || queue.size() > 0 || pending.size() > 0)
			{
				// drain stuff to pending
				queue.drainTo(pending);
				
				if(pending.size() == 0) 
				{ 
					// poll 
					try 
					{
						ScheduledOperation sop = queue.poll(5, TimeUnit.MILLISECONDS);
						if(sop != null) { pending.add(sop); }
					} 
					catch (InterruptedException e1) {	e1.printStackTrace(); }
					continue; 
				}
				Iterator<ScheduledOperation> iter = pending.iterator();
				while(iter.hasNext() == true)
				{
					ScheduledOperation sop = iter.next();
					if(sop.isReady() == true)
					{						
						iter.remove(); // remove
						
						complete(sop); // complete the operation
					}
				}
			}
		}
		
		public void shutdown()
		{
			done = true;
		}
	}
	
	protected BlockingQueue<ScheduledOperation> scheduledQueue = new LinkedBlockingQueue<ScheduledOperation>();
	
	public StorageAdapter(InternalStorageInterface s)
	{
		storage = s; opened = false;
		completionThread = new CompletionThread(scheduledQueue);
	}
	
	protected void complete(ScheduledOperation sop)
	{
		assert(sop.isReady() == true);
		//pe.completeOperation(sop); // -----------------
		
	}
	
	@Override
	public void connect() 
	{
		completionThread.start();
		//pe.connectCall(); // -----------
		
		
		storage.connect();
		
	
		//pe.connectDone(); // -----------
		opened = true;
	}

	@Override
	public ScheduledOperation downloadObject(DownloadObject op) 
	{
		assert(opened == true);
		assert(completionThread.isAlive() == true);
		//pe.scheduleOperation(op); // -----------
		
		
		ScheduledOperation sop = storage.downloadObject(op);
		
		try { scheduledQueue.put(sop); } catch (InterruptedException e) { e.printStackTrace(); }
		
		return sop;
	}

	@Override
	public ScheduledOperation uploadObject(UploadObject op) 
	{
		assert(opened == true);
		assert(completionThread.isAlive() == true);
		//pe.scheduleOperation(op); // -----------	
	
		ScheduledOperation sop = storage.uploadObject(op);
		
		try { scheduledQueue.put(sop); } catch (InterruptedException e) { Errors.error(e); }
		
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteObject op)
	{
		assert(opened == true);
		assert(completionThread.isAlive() == true);
		
		//pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.deleteObject(op);
		
		try { scheduledQueue.put(sop); } catch (InterruptedException e) { Errors.error(e); }
		
		return sop;
	}
	
	@Override
	public ScheduledOperation copyObject(CopyObject op) 
	{
		assert(opened == true);
		assert(completionThread.isAlive() == true);
		
		//pe.scheduleOperation(op); // -----------
		
		ScheduledOperation sop = storage.copyObject(op);
		
		try { scheduledQueue.put(sop); } catch (InterruptedException e) { Errors.error(e); }
		
		return sop;
	}	
	

	@Override
	public void disconnect() 
	{
		assert(opened == true);
		assert(completionThread.isAlive() == true);
		
	
		completionThread.shutdown();
		try { completionThread.join(); } catch (InterruptedException e) { Errors.error(e); }
		
		// before we disconnect, let's get the total number of bytes used storage-side
		long bytes = storage.totalByteSize();
		
		storage.disconnect();
		
		
		opened = false;
	}

	@Override
	public void cloneTo(String to) { storage.cloneTo(to); }
}
