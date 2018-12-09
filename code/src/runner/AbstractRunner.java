package runner;

import java.io.File;


import crypto.CryptoProvider;
import Interfaces.CompletionCallback;
import Interfaces.ExternalClientInterface;
import Interfaces.ExternalStorageInterface;
import services.Request;
import services.ScheduledRequest;
import services.RequestSource;
import utils.*;
/**
 * Represents an abstract runner.
 * A runner is a class that runs (i.e., plays, or replays) requests.
 *
 */
public abstract class AbstractRunner 
{
	protected Log log = Log.getInstance();
	protected SystemParameters sysParams = SystemParameters.getInstance();
	protected ClientParameters clientParams = ClientParameters.getInstance();
	
	protected SessionState ss = SessionState.getInstance();
	
	protected CryptoProvider cp = CryptoProvider.getInstance();

	protected ExternalClientInterface client = null;
	protected ExternalStorageInterface storage = null;
	
	public AbstractRunner(ExternalClientInterface c, ExternalStorageInterface s)
	{
		client = c; storage = s;
	}
	
	public void open(File stateFile, RequestSource inputRS)
	{
		client.open(storage, stateFile, ss.shouldReset());
	}
	
	public int run(RequestSource rs, int length)
	{
		int processed = 0;
		
		while(rs.hasNext() == true && processed < length)
		{
			Request req = rs.next();
			
			CompletionCallback callback = onNew(req);
			
			ScheduledRequest sreq = client.schedule(req, callback);
			
			boolean success = onScheduled(sreq);
			if(success == true)
			{
				processed++;
				ss.nextReqId++;
			}
			else
			{
				processFailure(sreq);
			}
		}
		
		return processed;
	}
	
	public abstract boolean onScheduled(ScheduledRequest sreq);

	public abstract CompletionCallback onNew(Request req);// { return null; }

	public void processFailure(ScheduledRequest sreq)
	{
		Errors.error("[AR] Failed on post-process of " + sreq.getRequest().getStringDesc() + " !");
	}
	
	public void close(String cloneStorageTo)
	{
		client.close(cloneStorageTo);
	}
}
