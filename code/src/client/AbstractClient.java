package client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;

import Interfaces.*;
import services.GetRequest;
import services.PutRequest;
import services.ScheduledRequest;
import utils.*;


public abstract class AbstractClient implements InternalClientInterface {
	protected Log log = Log.getInstance();
	protected ClientParameters clientParams = ClientParameters.getInstance();
	protected SessionState ss = SessionState.getInstance();
	
	protected ExternalStorageInterface s = null;
	
	protected File stateFile = null;
	
	protected SecureRandom rng = new SecureRandom();
	
	
	
	protected abstract void load(ObjectInputStream is) throws Exception;
	
	protected void load()
	{
		try
		{
			FileInputStream fis = new FileInputStream(stateFile);
			ObjectInputStream is = new ObjectInputStream(fis);
			
			// scheme specific load
			load(is);
			
			is.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
	}
	
	protected abstract void save(ObjectOutputStream os) throws Exception;
	
	protected void save()
	{
		try
		{
			FileOutputStream fos = new FileOutputStream(stateFile);
			ObjectOutputStream os = new ObjectOutputStream(fos);
			
			// scheme specific save
			save(os);
			
			os.flush();
			os.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
	}
	 
	/** called when open is called, after everything else **/
	protected void init(boolean reset) {}
	
	@Override
	public void open(ExternalStorageInterface storage, File state, boolean reset) 
	{
		s = storage;
		
		stateFile = state;
		
		// restore the state if needed
		if(reset == false)
		{
			load();
		}
		
		// connect to the storage
		s.connect();
		
		init(reset);
	}

	@Override
	public abstract boolean isSynchronous();

	@Override
	public abstract String getName();

	@Override
	public abstract ScheduledRequest scheduleGet(GetRequest req);

	@Override
	public abstract ScheduledRequest schedulePut(PutRequest req);

	/** called when close is called, before anything else **/
	protected void shutdown() {}
	
	@Override
	public void close(String cloneStorageTo) 
	{
		shutdown();
		
		if(cloneStorageTo != null) { s.cloneTo(cloneStorageTo); } // clone if needed
		
		s.disconnect(); 
		
		save(); // save the state
	}

}
