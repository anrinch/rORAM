package Interfaces;
import services.*;


public interface ExternalStorageInterface {

public void connect();
	
	public ScheduledOperation downloadObject(DownloadObject op);
	public ScheduledOperation uploadObject(UploadObject op);
	public ScheduledOperation deleteObject(DeleteObject op);
	public ScheduledOperation copyObject(CopyObject op);
	
	public void disconnect();
	
	public void cloneTo(String to);
	
}
