package backend;

import Interfaces.*;
import services.CopyObject;
import services.DeleteObject;
import services.DownloadObject;
import services.Service;
import services.Service.ServiceType;
import services.ScheduledOperation;
import services.UploadObject;
import utils.AmazonS3Utils;
import utils.Errors;
import utils.Log;
import utils.SessionState;
import utils.SystemParameters;
import data.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.NoHeadsBeforeGetsTransferManager;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * Implements an asynchronous storage interface for Amazon S3.
 * 
 * <p>
 * @see AmazonS3Storage
 */
public class AmazonS3AsyncStorage implements InternalStorageInterface 
{
	private Log log = Log.getInstance();
	private SystemParameters sysParams = SystemParameters.getInstance();
	
	private AmazonS3Utils utils = AmazonS3Utils.getInstance();
	
	private ExecutorService executor = Executors.newFixedThreadPool(128); // Executors.newCachedThreadPool(); 
	
	private AmazonS3Client s3 = null;
	
	private TransferManager tm = null;
	private NoHeadsBeforeGetsTransferManager pitm = null;
	
	private String bucketName = null;
	
	private boolean resetBucket = false;
	
	public AmazonS3AsyncStorage(boolean shouldReset) { resetBucket = shouldReset; }
	
	
	
	private AtomicInteger pendingOpsCount = new AtomicInteger(0);
	
	public class AmazonS3StorageCallback
	{
		private ScheduledOperation sop = null;
		private int attempt = 0;
		private DataItem dataItem = null;
		
		public AmazonS3StorageCallback(ScheduledOperation op) 
		{
			sop = op;
			attempt = 0;
		}
		
		public void setDataItem(DataItem d) { dataItem = d; }

		public void onSuccess() 
		{
			pendingOpsCount.decrementAndGet(); // decrement
			sop.onSuccess(dataItem);
			
			attempt++;
		}

		public void onFailure(Exception e) 
		{
			pendingOpsCount.decrementAndGet(); // decrement
			
			attempt++;
			boolean isDelete = sop.getOperation().getType() == ServiceType.DELETE;
			if(attempt >= sysParams.storageOpMaxAttempts) 
			{
				if(isDelete == true) { pendingOpsCount.incrementAndGet(); onSuccess(); } // fix for deletes of invalid keys -> make it a success
				else
				{
					sop.onFailure();
					Errors.error(e);
				}
			}
			else  // retry
			{
				Errors.warn(e);
				{
					long reqId = sop.getOperation().getRequestId();
					long opId = sop.getOperation().getOperationId();
					ServiceType opType = sop.getOperation().getType();
					String opKey = sop.getOperation().getKey();
					
					String msg = "[AsyncS3] Operation(" + opId + ", " + opType + ", " + opKey + ") from reqId: " + reqId;
					msg += " has failed (attempt: " + attempt + ") -> Retrying...";				
					Errors.warn(msg);
				}
				
				ServiceType type = sop.getOperation().getType();
				switch(type)
				{
				case DOWNLOAD: _downloadObject(sop, this); break;
				case UPLOAD: _uploadObject(sop, this); break;
				case DELETE: _deleteObject(sop, this); break;
				default:
					assert(true == false); Errors.error("Coding FAIL!"); break;
				}
				
				
			}
		}
	}

	@Override
	public void connect() 
	{
		bucketName = SessionState.getInstance().storageKey.toLowerCase();
		s3 = utils.initialize(sysParams.credentials, bucketName, resetBucket);
		
		tm = new TransferManager(s3, executor);
		pitm = new NoHeadsBeforeGetsTransferManager(s3, executor);
	}

	private void _downloadObject(ScheduledOperation sop, final AmazonS3StorageCallback listener)
	{
		assert(listener != null);
		DownloadObject op = (DownloadObject)sop.getOperation();
		final Transfer tr;
		// -d-
		//{ log.append("[AsyncS3] Downloading key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }
		
		try
		{	
			TempFileDataItem d = new TempFileDataItem();
			
			listener.setDataItem(d); // empty data item for uploads
			
			final int byteSize = 1;

			if(op.getObjectType() == Service.ObjectType.Block || op.getObjectType() == Service.ObjectType.Metadata){
				GetObjectRequest rangeObjectRequest = new GetObjectRequest(
						bucketName, op.getKey());
				rangeObjectRequest.setRange(op.getObjectOffset(), op.getObjectOffset()+op.getObjectSize());
				tr = pitm.download(rangeObjectRequest, d.getFile(), byteSize);
			}
			else{
				tr = pitm.download(bucketName, op.getKey(), d.getFile(), byteSize);
			}
			pendingOpsCount.incrementAndGet(); // increment the counter
			
			executor.submit(new Runnable() 
			{
				@Override
				public void run() 
				{
					try { tr.waitForCompletion(); listener.onSuccess();	} 
					catch (AmazonClientException | InterruptedException e) { listener.onFailure(e); }
				}
			});
		}
		catch (AmazonServiceException ase) { utils.processASE(ase); } 
		catch (AmazonClientException ace) { utils.processACE(ace); } 
	}
	
	@Override
	public ScheduledOperation downloadObject(DownloadObject op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		
		_downloadObject(sop, new AmazonS3StorageCallback(sop));
		
		return sop;
	}

	private void _uploadObject(ScheduledOperation sop, final AmazonS3StorageCallback listener)
	{
		assert(listener != null);
		UploadObject op = (UploadObject)sop.getOperation();
		
		// -d-
		//{ log.append("[AsyncS3] Uploading key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }	
		
		try
		{
			byte[] data = op.getDataItem().getData();
			ObjectMetadata metadata = new ObjectMetadata();
		    metadata.setContentLength(data.length);
		    
		    listener.setDataItem(new EmptyDataItem()); // empty data item for uploads
		    
			final Transfer tr = tm.upload(bucketName, op.getKey(), new ByteArrayInputStream(data), metadata);			
			pendingOpsCount.incrementAndGet(); // increment the counter
			
			executor.submit(new Runnable() 
			{
				@Override
				public void run() 
				{
					try { tr.waitForCompletion(); listener.onSuccess();	} 
					catch (AmazonClientException | InterruptedException e) { listener.onFailure(e); }
				}
			});
		}
		catch (AmazonServiceException ase) { utils.processASE(ase); } 
		catch (AmazonClientException ace) { utils.processACE(ace); } 
	}
	
	@Override
	public ScheduledOperation uploadObject(UploadObject op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		
		_uploadObject(sop, new AmazonS3StorageCallback(sop));
		
		return sop;
	}
	
	private void _deleteObject(final ScheduledOperation sop, final AmazonS3StorageCallback listener)
	{
		assert(listener != null);
		final DeleteObject op = (DeleteObject)sop.getOperation();
		
		// -d-
		//{ log.append("[AsyncS3] Deleting key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }	
				
				
		listener.setDataItem(new EmptyDataItem()); // empty data items for copies
		
		executor.submit(new Runnable() 
		{
			@Override
			public void run() 
			{
				try { s3.deleteObject(bucketName, op.getKey()); listener.onSuccess(); }
				catch (AmazonClientException e) { listener.onFailure(e); } 
			}
		});
	}

	@Override
	public ScheduledOperation deleteObject(DeleteObject op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		
		_deleteObject(sop, new AmazonS3StorageCallback(sop));
		
		return sop;
	}
	
	@Override
	public ScheduledOperation copyObject(CopyObject op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		
		_copyObject(sop, new AmazonS3StorageCallback(sop));
		
		return sop;
	}
	
	private void _copyObject(final ScheduledOperation sop, final AmazonS3StorageCallback listener)
	{
		assert(listener != null);
		final CopyObject op = (CopyObject)sop.getOperation();
		
		listener.setDataItem(new EmptyDataItem()); // empty data items for copies
		
		executor.submit(new Runnable() 
		{
			@Override
			public void run() 
			{
				try { s3.copyObject(bucketName, op.getSourceKey(), bucketName, op.getDestinationKey()); listener.onSuccess(); }
				catch (AmazonClientException e) { listener.onFailure(e); } 
			}
		});
	}
	
	
	

	@Override
	public void disconnect() 
	{
		while(pendingOpsCount.get() > 0)
		{
			try { Thread.sleep(5); } catch (InterruptedException e) { e.printStackTrace(); }
		}
		
		if(tm != null) { tm.shutdownNow(); }
		if(pitm != null) { pitm.shutdownNow(); }
		
		s3.shutdown();
		
		executor.shutdownNow();
		try { executor.awaitTermination(0, TimeUnit.SECONDS); } 
		catch (InterruptedException e) { e.printStackTrace(); }
	}

	@Override
	public long totalByteSize()
	{
		while(pendingOpsCount.get() > 0)
		{
			try { Thread.sleep(5); } catch (InterruptedException e) { e.printStackTrace(); }
		}
		return utils.bucketByteSize(s3, bucketName);
	}

	@Override
	public void cloneTo(String to)
	{
		while(pendingOpsCount.get() > 0)
		{
			try { Thread.sleep(5); } catch (InterruptedException e) { e.printStackTrace(); }
		}
		utils.cloneBucket(s3, bucketName, to);
	}
}
