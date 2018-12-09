package Interfaces;


import java.io.File;
import java.util.Collection;
import java.util.List;

import services.Request;
import services.ScheduledRequest;

public interface ExternalClientInterface {
public void open(ExternalStorageInterface storage, File stateFile, boolean reset);
	
	public boolean isSynchronous();
	public String getName();
	
	public ScheduledRequest schedule(Request req, CompletionCallback callback);
	
	public void waitForCompletion(Collection<ScheduledRequest> reqs);
	
	public List<ScheduledRequest> getPendingRequests();
	
	public void close(String cloneStorageTo);
}
