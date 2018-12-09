package Interfaces;

import java.io.File;
import services.*;


public interface InternalClientInterface {
	public void open(ExternalStorageInterface storage, File stateFile, boolean reset);
	
	public boolean isSynchronous();
	public String getName();
	
	public ScheduledRequest scheduleGet(GetRequest req);
	public ScheduledRequest schedulePut(PutRequest req);

	public void close(String cloneStorageTo);

	public long peakByteSize();
}
