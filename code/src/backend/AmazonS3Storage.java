package backend;

import utils.*;
import data.*;
import Interfaces.*;
import services.*;
import utils.AmazonS3Utils;
import utils.Errors;
import utils.MiscUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Implements a (synchronous) storage interface for Amazon S3.
 * 
 *  @see AmazonS3AsyncStorage
 */
public class AmazonS3Storage implements InternalStorageInterface 
{
	private SystemParameters sysParams = SystemParameters.getInstance();
	private AmazonS3Utils utils = AmazonS3Utils.getInstance();
	
	private AmazonS3Client s3 = null;
	
	private String bucketName = null;
	
	private boolean resetBucket = false;
	
	public AmazonS3Storage(boolean shouldReset) { resetBucket = shouldReset; }

	@Override
	public void connect() 
	{
		bucketName = SessionState.getInstance().storageKey.toLowerCase();
		s3 = utils.initialize(sysParams.credentials, bucketName, resetBucket);
	}

	@Override
	public ScheduledOperation downloadObject(DownloadObject op) 
	{
		String key = op.getKey();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			S3Object o = null;
			try
			{
				if(op.getObjectType() == Service.ObjectType.Block || op.getObjectType() == Service.ObjectType.Metadata){
					GetObjectRequest rangeObjectRequest = new GetObjectRequest(
							bucketName, key);
					rangeObjectRequest.setRange(op.getObjectOffset(), op.getObjectOffset()+op.getObjectSize());
					o = s3.getObject(rangeObjectRequest);
					
				}
				else{
					o = s3.getObject(bucketName, key);
				}
				
				InputStream is = o.getObjectContent();
				SimpleDataItem sdi = new SimpleDataItem(MiscUtils.getInstance().ByteArrayFromInputStream(is));
				sop.onSuccess(sdi);
				try { is.close(); } catch (IOException e) { Errors.error(e); }
				
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation uploadObject(UploadObject op) 
	{
		String key = op.getKey();
		byte[] data = op.getDataItem().getData();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			
			try
			{
				ObjectMetadata metadata = new ObjectMetadata();
			    metadata.setContentLength(data.length);
			    
				PutObjectResult res = s3.putObject(bucketName, key, new ByteArrayInputStream(data), metadata);
				
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteObject op) 
	{
		String key = op.getKey();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			
			try
			{
				s3.deleteObject(bucketName, key);
				
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation copyObject(CopyObject op) 
	{
		String srcKey = op.getSourceKey();
		String destKey = op.getDestinationKey();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			
			try
			{
				s3.copyObject(bucketName, srcKey, bucketName, destKey);
				
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}
	
	
	

	@Override
	public void disconnect() { s3.shutdown(); }
	
	@Override
	public long totalByteSize()
	{
		return utils.bucketByteSize(s3, bucketName);
	}

	@Override
	public void cloneTo(String to) 
	{
		utils.cloneBucket(s3, bucketName, to);
	}
}
