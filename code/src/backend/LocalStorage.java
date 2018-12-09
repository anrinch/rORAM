package backend;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import services.CopyObject;
import services.DeleteObject;
import services.DownloadObject;
import services.ScheduledOperation;
import services.Service.ObjectType;
import services.UploadObject;
import Interfaces.*;
import utils.*;
import data.*;


public class LocalStorage implements InternalStorageInterface{

	private Log log = Log.getInstance();
	
	private SystemParameters sysParams = SystemParameters.getInstance();
	private String directoryFP = null;
	
	private boolean reset = false;
	
	public LocalStorage() { this(true); }
	
	public LocalStorage(String dirFP, boolean rst) { directoryFP = dirFP; reset = rst; }
	
	public LocalStorage(boolean rst) { this(null, rst); }
	
	private void initialize(String dirFP) 
	{
		directoryFP = dirFP;
		
		FileUtils.getInstance().initializeDirectory(directoryFP, reset);
	}	

	@Override
	public void connect() 
	{
		initialize((directoryFP == null) ? sysParams.localDirectoryFP : directoryFP);

	}

	@Override
	public ScheduledOperation downloadObject(DownloadObject op) 
	{
		String key = op.getKey();
		ObjectType objectType = op.getObjectType();
		int objectStartOffset = op.getObjectOffset();
		int objectSize = op.getObjectSize();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String fp = directoryFP + "/" + key;
			try 
			{
				
				if(objectType == ObjectType.Metadata || objectType == ObjectType.Block){
					// Need an object from bucket
					
					RandomAccessFile f = new RandomAccessFile(fp,"r"); byte[] d = new byte[(int) f.length()];
					f.seek(objectStartOffset);
					byte[] b = new byte[objectSize];
					f.read(b);
					f.close();
					sop.onSuccess(new SimpleDataItem(b));
					
				}
				
				else
				{
						
						// Need the bucket //
						File f = new File(fp);
						FileInputStream fi = new FileInputStream(fp);
						byte[] d = new byte[(int) f.length()];
						fi.read(d);
						fi.close();
						sop.onSuccess(new SimpleDataItem(d));
				}
				} 
			catch (IOException e) 
			{
				e.printStackTrace();
				//throw new RuntimeException(e.getMessage());
				sop.onFailure();
			}
		}
		
		return sop;
	}

	@Override
	public ScheduledOperation uploadObject(UploadObject op) 
	{
		String key = op.getKey();
		byte[] data = op.getDataItem().getData();
		ObjectType objectType = op.getObjectType();
		int objectStartOffset = op.getObjectOffset();
		int objectSize = op.getObjectSize();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String fp = directoryFP + "/" + key;
			try 
			{
				
				if(objectType == ObjectType.Metadata || objectType == ObjectType.Block){
					// Need an object from bucket
					
					RandomAccessFile f = new RandomAccessFile(fp,"rw");
					f.seek(objectStartOffset);
					
					f.write(data);
					f.close();
					sop.onSuccess(new EmptyDataItem());
					
				}
				
				else{
				// -d-
				//{ log.append("[LS (uploadObject)] uploading key: " + key, Log.TRACE); } // debug only
				
				FileOutputStream f = new FileOutputStream(fp);
				f.write(data);
				f.close();
				
				sop.onSuccess(new EmptyDataItem());
				}
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
				//throw new RuntimeException(e.getMessage());
				sop.onFailure();
			}
		}
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteObject op) 
	{
		String key = op.getKey();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			//{ log.append("[LS (deleteObject)] deleting key: " + key, Log.TRACE); } // debug only
			
			String fp = directoryFP + "/" + key;
			File f = new File(fp); 
			
			if(f.delete() == true) { sop.onSuccess(new EmptyDataItem()); }
			else { sop.onFailure(); }
		}
		return sop;
	}
	
	
	@Override
	public ScheduledOperation copyObject(CopyObject op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String srcFP = directoryFP + "/" + op.getSourceKey(); File srcFile = new File(srcFP); 
			String destFP = directoryFP + "/" + op.getDestinationKey(); File destFile = new File(destFP); 
			
			boolean success = FileUtils.getInstance().copy(srcFile, destFile);
			if(success == true) { sop.onSuccess(new EmptyDataItem()); }
			else { sop.onFailure(); }
		}
		return sop;
	}


	@Override
	public void disconnect() { ; }

	@Override
	public long totalByteSize() 
	{
		return FileUtils.getInstance().flatDirectoryByteSize(directoryFP);
	}
	
	@Override
	public void cloneTo(String to) 
	{
		FileUtils.getInstance().flatDirectoryCopy(directoryFP, to);
	}

}
