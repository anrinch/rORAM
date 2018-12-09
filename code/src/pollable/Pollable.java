package pollable;

import java.util.Collection;
import java.util.Iterator;
import utils.Errors;

/**
 * Represents something that can be polled for readiness / completion (e.g., an operation)
 *
 */
public abstract class Pollable 
{
	public final static int defaultSleepTimeMs = 10;
	
	public abstract boolean isReady();
	
	public synchronized void waitUntilReady() 
	{
		while(isReady() == false)
		{
			try { wait(defaultSleepTimeMs); } catch (InterruptedException e) { Errors.error(e); }
		}
	}
	
	public static void waitForCompletion(Collection<? extends Pollable> v)
	{
		boolean done = false;
		while(done == false)
		{
			done = true;
			for(Pollable p : v) { if(p.isReady() == false) { done = false; break; } }
			
			if(done == false) { try { Thread.sleep(defaultSleepTimeMs); } catch (InterruptedException e) { Errors.error(e); } }
		}
	}

	public static void removeCompleted(Collection<? extends Pollable> v) 
	{
		Iterator<? extends Pollable> iter = v.iterator();
		while(iter.hasNext())
		{
			Pollable p = iter.next();
			if(p.isReady() == true) { iter.remove(); }
		}
	}
}