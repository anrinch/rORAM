package Interfaces;

import services.*;

public interface InternalStorageInterface {
	public void connect();
	
	public ScheduledOperation downloadObject(DownloadObject op);
	public ScheduledOperation uploadObject(UploadObject op);
	public ScheduledOperation deleteObject(DeleteObject op);
	public ScheduledOperation copyObject(CopyObject op);
	public void disconnect();
	
	public long totalByteSize();
	
	public void cloneTo(String to);
}
