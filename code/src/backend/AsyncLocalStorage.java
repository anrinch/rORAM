package backend;

import Interfaces.*;
import services.CopyObject;
import services.DeleteObject;
import services.DownloadObject;
import services.Service.ServiceType;
import services.ScheduledOperation;
import services.UploadObject;
import utils.Errors;
import utils.FileUtils;
import utils.SystemParameters;
import data.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;

import Interfaces.InternalStorageInterface;

/**
 * Implements an asynchronous storage interface for the local filesystem.
 * <p><p>
 * The primary use of storage interfaces for the local filesystem is testing/debugging. 
 * In other words, this class can be used to test new (ORAM) clients locally before running them on S3.
 * <p>
 * @see LocalStorage
 */
public class AsyncLocalStorage implements InternalStorageInterface 
{	
	public class AsyncLocalStorageCompletionHandler 
		implements CompletionHandler<Integer, AsynchronousFileChannel>
	{
		private ScheduledOperation sop = null;
		private ByteBuffer buffer = null;
		
		public AsyncLocalStorageCompletionHandler(ScheduledOperation op, ByteBuffer b) 
		{
			sop = op;
			buffer = b;
		}
		
		
		@Override
		public void completed(Integer ret, AsynchronousFileChannel channel) 
		{
			try { channel.close(); } catch (IOException e) { Errors.error(e); }
			
			// add random delay
			if(maxDelay > 0)
			{
				int sleepTime = minDelay + new Random().nextInt(maxDelay - minDelay);
				try { Thread.sleep(sleepTime); } catch (InterruptedException e) { Errors.error(e); }
			}
			
			DataItem d = null;
			if(sop.getOperation().getType() == ServiceType.DOWNLOAD) { d = new SimpleDataItem(buffer.array()); }
			else { d = new EmptyDataItem(); }
			
			sop.onSuccess(d);
		}

		@Override
		public void failed(Throwable ex, AsynchronousFileChannel channel) 
		{
			try { channel.close(); } catch (IOException e) { e.printStackTrace(); }
			
			sop.onFailure();
			
			throw new RuntimeException(ex);
		}
		
	}
	
	private SystemParameters sysParams = SystemParameters.getInstance();
	private String directoryFP = null;
	
	private boolean reset = false;
	private int minDelay = 0;
	private int maxDelay = 0;
	
	public AsyncLocalStorage() { this(true); }
	
	public AsyncLocalStorage(String dirFP, boolean rst) { this(dirFP, rst, 0); }
	
	public AsyncLocalStorage(boolean rst) { this(null, rst); }
	
	public AsyncLocalStorage(String dirFP, boolean rst, int delay) 
	{
		directoryFP = dirFP; reset = rst; 
		Errors.verify(delay >= 0); minDelay = delay / 2; maxDelay = delay;
		Errors.verify((maxDelay == 0 && minDelay == 0) || (maxDelay > minDelay));
	}

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
		String fp = directoryFP + "/" + op.getKey();
		
		File f = new File(fp); 
		long size = f.length(); assert(size <= (long)Integer.MAX_VALUE);
		byte[] buf = new byte[(int)size];
		ByteBuffer buffer = ByteBuffer.wrap(buf);
		
		ScheduledOperation sop = new ScheduledOperation(op);
		
		Path file = Paths.get(f.getAbsolutePath());
		try 
		{
			AsynchronousFileChannel afc = AsynchronousFileChannel.open(file, StandardOpenOption.READ);
			afc.read(buffer, 0, afc, new AsyncLocalStorageCompletionHandler(sop, buffer));
		} 
		catch (IOException e) { throw new RuntimeException(e); }
		
		return sop;
	}

	@Override
	public ScheduledOperation uploadObject(UploadObject op) 
	{
		String fp = directoryFP + "/" + op.getKey();
		
		ByteBuffer buffer = ByteBuffer.wrap(op.getDataItem().getData());
		
		ScheduledOperation sop = new ScheduledOperation(op);
		
		Path file = Paths.get(fp);
		try 
		{
			AsynchronousFileChannel afc = AsynchronousFileChannel.open(file, StandardOpenOption.WRITE, 
													StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
			afc.write(buffer, 0, afc, new AsyncLocalStorageCompletionHandler(sop, buffer));
		} 
		catch (IOException e) { throw new RuntimeException(e); }
		
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteObject op) 
	{
		String fp = directoryFP + "/" + op.getKey();
		File f = new File(fp);
		boolean success = f.delete();
		
		ScheduledOperation sop = new ScheduledOperation(op);
		if(success == true) { sop.onSuccess(null); } else { sop.onFailure(); }
		
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
		try { Thread.sleep(50); } catch (InterruptedException e) { e.printStackTrace(); }
		return FileUtils.getInstance().flatDirectoryByteSize(directoryFP);
	}

	@Override
	public void cloneTo(String to) 
	{
		try { Thread.sleep(50); } catch (InterruptedException e) { e.printStackTrace(); }
		FileUtils.getInstance().flatDirectoryCopy(directoryFP, to);
	}
}
