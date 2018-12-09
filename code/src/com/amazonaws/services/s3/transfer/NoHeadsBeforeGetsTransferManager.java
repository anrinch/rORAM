//package eoram.cloudexp;

/* Modified from Amazon AWS Java SDK */
package com.amazonaws.services.s3.transfer;

//import com.amazonaws.services.s3.transfer.*;

/*
 * Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import static com.amazonaws.services.s3.internal.ServiceUtils.APPEND_MODE;
import static com.amazonaws.services.s3.internal.ServiceUtils.OVERWRITE_MODE;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventFilter;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressListenerChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3Encryption;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.internal.DownloadImpl;
import com.amazonaws.services.s3.transfer.internal.DownloadMonitor;
import com.amazonaws.services.s3.transfer.internal.MultipleFileDownloadImpl;
import com.amazonaws.services.s3.transfer.internal.MultipleFileTransferMonitor;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListenerChain;
import com.amazonaws.services.s3.transfer.internal.TransferManagerUtils;
import com.amazonaws.services.s3.transfer.internal.TransferProgressUpdatingListener;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import com.amazonaws.util.VersionInfoUtils;

/**
 * High level utility for managing transfers to Amazon S3.
 * <p>
 * <code>TransferManager</code> provides a simple API for uploading content to
 * Amazon S3, and makes extensive use of Amazon S3 multipart uploads to achieve
 * enhanced throughput, performance and reliability.
 * <p>
 * When possible, <code>TransferManager</code> attempts to use multiple threads
 * to upload multiple parts of a single upload at once. When dealing with large
 * content sizes and high bandwidth, this can have a significant increase on
 * throughput.
 * <p>
 * <code>TransferManager</code> is responsible for managing resources such as
 * connections and threads; share a single instance of
 * <code>TransferManager</code> whenever possible. <code>TransferManager</code>,
 * like all the client classes in the AWS SDK for Java, is thread safe. Call
 * <code> TransferManager.shutdownNow()</code> to release the resources once the
 * transfer is complete.
 * <p>
 * Using <code>TransferManager</code> to upload options to Amazon S3 is easy:
 *
 * <pre class="brush: java">
 * DefaultAWSCredentialsProviderChain credentialProviderChain = new DefaultAWSCredentialsProviderChain();
 * TransferManager tx = new TransferManager(
 * 		credentialProviderChain.getCredentials());
 * Upload myUpload = tx.upload(myBucket, myFile.getName(), myFile);
 *
 * // You can poll your transfer's status to check its progress
 * if (myUpload.isDone() == false) {
 * 	System.out.println(&quot;Transfer: &quot; + myUpload.getDescription());
 * 	System.out.println(&quot;  - State: &quot; + myUpload.getState());
 * 	System.out.println(&quot;  - Progress: &quot;
 * 			+ myUpload.getProgress().getBytesTransferred());
 * }
 *
 * // Transfers also allow you to set a &lt;code&gt;ProgressListener&lt;/code&gt; to receive
 * // asynchronous notifications about your transfer's progress.
 * myUpload.addProgressListener(myProgressListener);
 *
 * // Or you can block the current thread and wait for your transfer to
 * // to complete. If the transfer fails, this method will throw an
 * // AmazonClientException or AmazonServiceException detailing the reason.
 * myUpload.waitForCompletion();
 *
 * // After the upload is complete, call shutdownNow to release the resources.
 * tx.shutdownNow();
 * </pre>
 * <p>
 * Transfers can be paused and resumed at a later time. It can also survive JVM
 * crash, provided the information that is required to resume the transfer is
 * given as input to the resume operation. For more information on pause and resume,
 * @see Upload#pause()
 * @see Download#pause()
 * @see TransferManager#resumeUpload(PersistableUpload)
 * @see TransferManager#resumeDownload(PersistableDownload)
 */

/**
 * Same functionality as the AWS' S3 SDK {@link TransferManager} but makes no HTTP HEAD requests prior to an HTTP GET (for downloads).
 */
public class NoHeadsBeforeGetsTransferManager {

    /** The low level client we use to make the actual calls to Amazon S3. */
    private final AmazonS3 s3;

    /** Configuration for how TransferManager processes requests. */
    private TransferManagerConfiguration configuration;
    /** The thread pool in which transfers are uploaded or downloaded. */
    private final ExecutorService threadPool;

    /** Thread used for periodicially checking transfers and updating thier state. */
    private final ScheduledExecutorService timedThreadPool = new ScheduledThreadPoolExecutor(1, daemonThreadFactory);

    private static final Log log = LogFactory.getLog(TransferManager.class);

    /**
     * Constructs a new <code>TransferManager</code> and Amazon S3 client using
     * the credentials from <code>DefaultAWSCredentialsProviderChain</code>
     * <p>
     * <code>TransferManager</code> and client objects may pool connections and
     * threads. Reuse <code>TransferManager</code> and client objects and share
     * them throughout applications.
     * <p>
     * TransferManager and all AWS client objects are thread safe.
     */
    public NoHeadsBeforeGetsTransferManager(){
        this(new AmazonS3Client(new DefaultAWSCredentialsProviderChain()));
    }

    /**
     * Constructs a new <code>TransferManager</code> and Amazon S3 client using
     * the specified AWS security credentials provider.
     * <p>
     * <code>TransferManager</code> and client objects may pool connections and
     * threads. Reuse <code>TransferManager</code> and client objects and share
     * them throughout applications.
     * <p>
     * TransferManager and all AWS client objects are thread safe.
     *
     * @param credentialsProvider
     *            The AWS security credentials provider to use when making
     *            authenticated requests.
     */
    public NoHeadsBeforeGetsTransferManager(AWSCredentialsProvider credentialsProvider) {
        this(new AmazonS3Client(credentialsProvider));
    }

    /**
     * Constructs a new <code>TransferManager</code> and Amazon S3 client using
     * the specified AWS security credentials.
     * <p>
     * <code>TransferManager</code> and client objects
     * may pool connections and threads.
     * Reuse <code>TransferManager</code> and client objects
     * and share them throughout applications.
     * <p>
     * TransferManager and all AWS client objects are thread safe.
     *
     * @param credentials
     *            The AWS security credentials to use when making authenticated
     *            requests.
     */
    public NoHeadsBeforeGetsTransferManager(AWSCredentials credentials) {
        this(new AmazonS3Client(credentials));
    }

    /**
     * Constructs a new <code>TransferManager</code>,
     * specifying the client to use when making
     * requests to Amazon S3.
     * <p>
     * <code>TransferManager</code> and client objects
     * may pool connections and threads.
     * Reuse <code>TransferManager</code> and client objects
     * and share them throughout applications.
     * <p>
     * TransferManager and all AWS client objects are thread safe.
     * </p>
     *
     * @param s3
     *            The client to use when making requests to Amazon S3.
     */
    public NoHeadsBeforeGetsTransferManager(AmazonS3 s3) {
        this(s3, TransferManagerUtils.createDefaultExecutorService());
    }

    /**
     * Constructs a new <code>TransferManager</code>
     * specifying the client and thread pool to use when making
     * requests to Amazon S3.
     * <p>
     * <code>TransferManager</code> and client objects
     * may pool connections and threads.
     * Reuse <code>TransferManager</code> and client objects
     * and share them throughout applications.
     * <p>
     * TransferManager and all AWS client objects are thread safe.
     *
     * @param s3
     *            The client to use when making requests to Amazon S3.
     * @param threadPool
     *            The thread pool in which to execute requests.
     */
    public NoHeadsBeforeGetsTransferManager(AmazonS3 s3, ExecutorService threadPool) {
        this.s3 = s3;
        this.threadPool = threadPool;
        this.configuration = new TransferManagerConfiguration();
    }


    /**
     * Sets the configuration which specifies how
     * this <code>TransferManager</code> processes requests.
     *
     * @param configuration
     *            The new configuration specifying how
     *            this <code>TransferManager</code>
     *            processes requests.
     */
    public void setConfiguration(TransferManagerConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Returns the configuration which specifies how
     * this <code>TransferManager</code> processes requests.
     *
     * @return The configuration settings for this <code>TransferManager</code>.
     */
    public TransferManagerConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Returns the underlying Amazon S3 client used to make requests to
     * Amazon S3.
     *
     * @return The underlying Amazon S3 client used to make requests to
     *         Amazon S3.
     */
    public AmazonS3 getAmazonS3Client() {
        return s3;
    }

    /**
     * Schedules a new transfer to download data from Amazon S3 and save it to
     * the specified file. This method is non-blocking and returns immediately
     * (i.e. before the data has been fully downloaded).
     * <p>
     * Use the returned Download object to query the progress of the transfer,
     * add listeners for progress events, and wait for the download to complete.
     *
     * @param bucket
     *            The name of the bucket containing the object to download.
     * @param key
     *            The key under which the object to download is stored.
     * @param file
     *            The file to download the object's data to.
     *
     * @return A new <code>Download</code> object to use to check the state of
     *         the download, listen for progress notifications, and otherwise
     *         manage the download.
     *
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in Amazon S3 while processing the
     *             request.
     */
    public Download download(String bucket, String key, File file) {
        return download(new GetObjectRequest(bucket, key), file, 0);
    }

    /** NEW IMPL **/
    public Download download(String bucket, String key, File file, long byteSize) {
        return download(new GetObjectRequest(bucket, key), file, byteSize);
    }
    
    /**
     * Schedules a new transfer to download data from Amazon S3 and save it to
     * the specified file. This method is non-blocking and returns immediately
     * (i.e. before the data has been fully downloaded).
     * <p>
     * Use the returned Download object to query the progress of the transfer,
     * add listeners for progress events, and wait for the download to complete.
     *
     * @param getObjectRequest
     *            The request containing all the parameters for the download.
     * @param file
     *            The file to download the object data to.
     * @param byteSize 
     *
     * @return A new <code>Download</code> object to use to check the state of
     *         the download, listen for progress notifications, and otherwise
     *         manage the download.
     *
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in Amazon S3 while processing the
     *             request.
     */
    public Download download(final GetObjectRequest getObjectRequest, final File file, long byteSize) {
        return doDownload(getObjectRequest, file, null, null, OVERWRITE_MODE, byteSize);
    }

    /**
     * Schedules a new transfer to download data from Amazon S3 and save it to
     * the specified file. This method is non-blocking and returns immediately
     * (i.e. before the data has been fully downloaded).
     * <p>
     * Use the returned Download object to query the progress of the transfer,
     * add listeners for progress events, and wait for the download to complete.
     *
     * @param getObjectRequest
     *            The request containing all the parameters for the download.
     * @param file
     *            The file to download the object data to.
     * @param progressListener
     *            An optional callback listener to get the progress of the
     *            download.
     *
     * @return A new <code>Download</code> object to use to check the state of
     *         the download, listen for progress notifications, and otherwise
     *         manage the download.
     *
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in Amazon S3 while processing the
     *             request.
     */
    public Download download(final GetObjectRequest getObjectRequest,
            final File file, final S3ProgressListener progressListener) {
        return doDownload(getObjectRequest, file, null, progressListener,
                OVERWRITE_MODE, 0);
    }

    /**
     * Same as public interface, but adds a state listener so that callers can
     * be notified of state changes to the download.
     *
     * @see TransferManager#download(GetObjectRequest, File)
     */
    private Download doDownload(final GetObjectRequest getObjectRequest,
            final File file, final TransferStateChangeListener stateListener,
            final S3ProgressListener s3progressListener,
            final boolean resumeExistingDownload, final long byteSize) {

        appendSingleObjectUserAgent(getObjectRequest);

        String description = "Downloading from " + getObjectRequest.getBucketName() + "/" + getObjectRequest.getKey();

        TransferProgress transferProgress = new TransferProgress();
        // S3 progress listener to capture the persistable transfer when available
        S3ProgressListenerChain listenerChain = new S3ProgressListenerChain(
            // The listener for updating transfer progress
            new TransferProgressUpdatingListener(transferProgress),
            getObjectRequest.getGeneralProgressListener(),
            s3progressListener);           // Listeners included in the original request

        // The listener chain used by the low-level GetObject request.
        // This listener chain ignores any COMPLETE event, so that we could
        // delay firing the signal until the high-level download fully finishes.
        ProgressListenerChain listeners = new ProgressListenerChain(
                new ProgressEventFilter() {
                    @Override
                    public ProgressEvent filter(ProgressEvent progressEvent) {
                        // Block COMPLETE events from the low-level GetObject operation,
                        // but we still want to keep the BytesTransferred
                        return progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT
                             ? null // discard this event
                             : progressEvent
                             ;
                    }
                }, listenerChain);
        getObjectRequest.setGeneralProgressListener(listeners);
        GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(
                getObjectRequest.getBucketName(), getObjectRequest.getKey());
        if (getObjectRequest.getSSECustomerKey() != null) {
            getObjectMetadataRequest.setSSECustomerKey(getObjectRequest.getSSECustomerKey());
        }
        
        // MODIFIED HERE - get metadata only if byteSize == 0
        ObjectMetadata objectMetadata = null;
        
        if(byteSize == 0) { objectMetadata = s3.getObjectMetadata(getObjectMetadataRequest); }

        // We still pass the unfiltered listener chain into DownloadImpl
        final DownloadImpl download = new DownloadImpl(description,
                transferProgress, listenerChain, null, stateListener,
                getObjectRequest, file);

        long startingByte = 0;
        long lastByte = 0;
        
        if(byteSize == 0) { lastByte = objectMetadata.getContentLength(); }
        else { lastByte = byteSize; }

        if (getObjectRequest.getRange() != null
                && getObjectRequest.getRange().length == 2) {
            startingByte = getObjectRequest.getRange()[0];
            lastByte = getObjectRequest.getRange()[1];
        }

        long totalBytesToDownload = lastByte - startingByte;
        transferProgress.setTotalBytesToTransfer(totalBytesToDownload);

        if (resumeExistingDownload) {
            if (file.exists()) {
                long numberOfBytesRead = file.length();
                startingByte = startingByte + numberOfBytesRead;
                getObjectRequest.setRange(startingByte, lastByte);
                transferProgress.updateProgress(Math.min(numberOfBytesRead,
                        totalBytesToDownload));
                totalBytesToDownload = lastByte - startingByte;
            }
        }

        if (totalBytesToDownload < 0) {
            throw new IllegalArgumentException(
                    "Unable to determine the range for download operation.");
        }

        final CountDownLatch latch = new CountDownLatch(1);
        Future<?> future = submitDownloadTask(getObjectRequest, file,
                resumeExistingDownload, latch, download);
        download.setMonitor(new DownloadMonitor(download, future));
        latch.countDown();
        return download;
    }

    private Future<?> submitDownloadTask(
            final GetObjectRequest getObjectRequest, final File file,
            final boolean resumeExistingDownload,
            final CountDownLatch latch,
            final DownloadImpl download) {
        Future<?> future = threadPool.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    latch.await();
                    download.setState(TransferState.InProgress);
                    S3Object s3Object = ServiceUtils.retryableDownloadS3ObjectToFile(file,
                        new ServiceUtils.RetryableS3DownloadTask() {
                            @Override
                            public S3Object getS3ObjectStream() {
                                S3Object s3Object = s3.getObject(getObjectRequest);
                                download.setS3Object(s3Object);
                                return s3Object;
                            }

                            @Override
                            public boolean needIntegrityCheck() {
                                // Don't perform the integrity check
                                // if the stream data is wrapped
                                // in a decryption stream, or if
                                // we're only looking at a range of
                                // the data, since otherwise the
                                // checksum won't match up.
                                boolean performIntegrityCheck = true;
                                if (getObjectRequest.getRange() != null)
                                    performIntegrityCheck = false;
                                if (s3 instanceof AmazonS3Encryption)
                                    performIntegrityCheck = false;
                                return performIntegrityCheck;
                            }
                        }, resumeExistingDownload);

                    if (s3Object == null) {
                        download.setState(TransferState.Canceled);
                        download.setMonitor(new DownloadMonitor(download, null));
                        return download;
                    }

                    download.setState(TransferState.Completed);
                    return true;
                } catch (Throwable t) {
                    // Downloads aren't allowed to move from canceled to failed
                    if (download.getState() != TransferState.Canceled) {
                        download.setState(TransferState.Failed);
                    }
                    if (t instanceof Exception)
                        throw (Exception) t;
                    else
                        throw (Error) t;
                }
            }
        });
        return future;
    }

    /**
     * Downloads all objects in the virtual directory designated by the
     * keyPrefix given to the destination directory given. All virtual
     * subdirectories will be downloaded recursively.
     *
     * @param bucketName
     *            The bucket containing the virtual directory
     * @param keyPrefix
     *            The key prefix for the virtual directory, or null for the
     *            entire bucket. All subdirectories will be downloaded
     *            recursively.
     * @param destinationDirectory
     *            The directory to place downloaded files. Subdirectories will
     *            be created as necessary.
     */
    public MultipleFileDownload downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory) {
        if ( keyPrefix == null )
            keyPrefix = "";
        List<S3ObjectSummary> objectSummaries = new LinkedList<S3ObjectSummary>();
        Stack<String> commonPrefixes = new Stack<String>();
        commonPrefixes.add(keyPrefix);
        long totalSize = 0;
        // Recurse all virtual subdirectories to get a list of object summaries.
        // This is a depth-first search.
        do {
            String prefix = commonPrefixes.pop();
            ObjectListing listObjectsResponse = null;

            do {
                if ( listObjectsResponse == null ) {
                    ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
                            .withDelimiter(DEFAULT_DELIMITER).withPrefix(prefix);
                    listObjectsResponse = s3.listObjects(listObjectsRequest);
                } else {
                    listObjectsResponse = s3.listNextBatchOfObjects(listObjectsResponse);
                }

                for ( S3ObjectSummary s : listObjectsResponse.getObjectSummaries() ) {
                    // Skip any files that are also virtual directories, since
                    // we can't save both a directory and a file of the same
                    // name.
                    if ( !s.getKey().equals(prefix)
                            && !listObjectsResponse.getCommonPrefixes().contains(s.getKey() + DEFAULT_DELIMITER) ) {
                        objectSummaries.add(s);
                        totalSize += s.getSize();
                    } else {
                        log.debug("Skipping download for object " + s.getKey()
                                + " since it is also a virtual directory");
                    }
                }

                commonPrefixes.addAll(listObjectsResponse.getCommonPrefixes());
            } while ( listObjectsResponse.isTruncated() );
        } while ( !commonPrefixes.isEmpty() );

        /* This is the hook for adding additional progress listeners */
        ProgressListenerChain additionalListeners = new ProgressListenerChain();

        TransferProgress transferProgress = new TransferProgress();
        transferProgress.setTotalBytesToTransfer(totalSize);
        /*
         * Bind additional progress listeners to this
         * MultipleFileTransferProgressUpdatingListener to receive
         * ByteTransferred events from each single-file download implementation.
         */
        ProgressListener listener = new MultipleFileTransferProgressUpdatingListener(
                transferProgress, additionalListeners);

        List<DownloadImpl> downloads = new ArrayList<DownloadImpl>();

        String description = "Downloading from " + bucketName + "/" + keyPrefix;
        final MultipleFileDownloadImpl multipleFileDownload = new MultipleFileDownloadImpl(description, transferProgress,
                additionalListeners, keyPrefix, bucketName, downloads);
        multipleFileDownload.setMonitor(new MultipleFileTransferMonitor(multipleFileDownload, downloads));

        final CountDownLatch latch = new CountDownLatch(1);
        MultipleFileTransferStateChangeListener transferListener =
                new MultipleFileTransferStateChangeListener(latch, multipleFileDownload);

        for ( S3ObjectSummary summary : objectSummaries ) {
            // TODO: non-standard delimiters
            File f = new File(destinationDirectory, summary.getKey());
            File parentFile = f.getParentFile();
            if ( !parentFile.exists() && !parentFile.mkdirs() ) {
                throw new RuntimeException("Couldn't create parent directories for " + f.getAbsolutePath());
            }

            // All the single-file downloads share the same
            // MultipleFileTransferProgressUpdatingListener and
            // MultipleFileTransferStateChangeListener
            downloads.add((DownloadImpl) doDownload(
                            new GetObjectRequest(summary.getBucketName(),
                                    summary.getKey())
                                    .<GetObjectRequest>withGeneralProgressListener(
                                            listener),
                            f,
                            transferListener, null, false, 0));
        }

        if ( downloads.isEmpty() ) {
            multipleFileDownload.setState(TransferState.Completed);
            return multipleFileDownload;
        }

        // Notify all state changes waiting for the downloads to all be queued
        // to wake up and continue.
        latch.countDown();
        return multipleFileDownload;
    }


    /**
     * Lists files in the directory given and adds them to the result list
     * passed in, optionally adding subdirectories recursively.
     */
    private void listFiles(File dir, List<File> results, boolean includeSubDirectories) {
        File[] found = dir.listFiles();
        if ( found != null ) {
            for ( File f : found ) {
                if (f.isDirectory()) {
                    if (includeSubDirectories) {
                        listFiles(f, results, includeSubDirectories);
                    }
                } else {
                    results.add(f);
                }
            }
        }
    }

    /**
     * <p>
     * Aborts any multipart uploads that were initiated before the specified date.
     * </p>
     * <p>
     * This method is useful for cleaning up any interrupted multipart uploads.
     * <code>TransferManager</code> attempts to abort any failed uploads,
     * but in some cases this may not be possible, such as if network connectivity
     * is completely lost.
     * </p>
     *
     * @param bucketName
     *            The name of the bucket containing the multipart uploads to
     *            abort.
     * @param date
     *            The date indicating which multipart uploads should be aborted.
     */
    public void abortMultipartUploads(String bucketName, Date date)
            throws AmazonServiceException, AmazonClientException {
        MultipartUploadListing uploadListing = s3.listMultipartUploads(appendSingleObjectUserAgent(
                new ListMultipartUploadsRequest(bucketName)));
        do {
            for (MultipartUpload upload : uploadListing.getMultipartUploads()) {
                if (upload.getInitiated().compareTo(date) < 0) {
                    s3.abortMultipartUpload(appendSingleObjectUserAgent(new AbortMultipartUploadRequest(
                            bucketName, upload.getKey(), upload.getUploadId())));
                }
            }

            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucketName)
                .withUploadIdMarker(uploadListing.getNextUploadIdMarker())
                .withKeyMarker(uploadListing.getNextKeyMarker());
            uploadListing = s3.listMultipartUploads(appendSingleObjectUserAgent(request));
        } while (uploadListing.isTruncated());
    }

    /**
     * Forcefully shuts down this TransferManager instance - currently executing
     * transfers will not be allowed to finish. It also by default shuts down
     * the underlying Amazon S3 client.
     *
     * @see #shutdownNow(boolean)
     */
    public void shutdownNow() {
        shutdownNow(true);
    }

    /**
     * Forcefully shuts down this TransferManager instance - currently executing
     * transfers will not be allowed to finish. Callers should use this method
     * when they either:
     * <ul>
     * <li>have already verified that their transfers have completed by checking
     * each transfer's state
     * <li>need to exit quickly and don't mind stopping transfers before they
     * complete.
     * </ul>
     * <p>
     * Callers should also remember that uploaded parts from an interrupted
     * upload may not always be automatically cleaned up, but callers can use
     * {@link #abortMultipartUploads(String, Date)} to clean up any upload
     * parts.
     *
     * @param shutDownS3Client
     *            Whether to shut down the underlying Amazon S3 client.
     */
    public void shutdownNow(boolean shutDownS3Client) {
        threadPool.shutdownNow();
        timedThreadPool.shutdownNow();

        if (shutDownS3Client) {
            if (s3 instanceof AmazonS3Client) {
                ((AmazonS3Client)s3).shutdown();
            }
        }
    }

    /**
     * Shutdown without interrupting the threads involved, so that, for example,
     * any upload in progress can complete without throwing
     * {@link AbortedException}.
     */
    private void shutdown() {
        threadPool.shutdown();
        timedThreadPool.shutdown();
    }

    public static <X extends AmazonWebServiceRequest> X appendSingleObjectUserAgent(X request) {
        request.getRequestClientOptions().appendUserAgent(USER_AGENT);
        return request;
    }

    public static <X extends AmazonWebServiceRequest> X appendMultipartUserAgent(X request) {
        request.getRequestClientOptions().appendUserAgent(USER_AGENT_MULTIPART);
        return request;
    }
    private static final String USER_AGENT = TransferManager.class.getName() + "/" + VersionInfoUtils.getVersion();
    private static final String USER_AGENT_MULTIPART = TransferManager.class.getName() + "_multipart/" + VersionInfoUtils.getVersion();


    private static final String DEFAULT_DELIMITER = "/";

    /**
     * There is no need for threads from timedThreadPool if there is no more running threads in current process,
     * so we need a daemon thread factory for it.
     */
    private static final ThreadFactory daemonThreadFactory = new ThreadFactory() {
        final AtomicInteger threadCount = new AtomicInteger( 0 );
        public Thread newThread(Runnable r) {
            int threadNumber = threadCount.incrementAndGet();
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("S3TransferManagerTimedThread-" + threadNumber);
            return thread;
        }
    };


    /**
     * Resumes an download operation. This download operation uses the same
     * configuration as the original download. Any data already fetched will be
     * skipped, and only the remaining data is retrieved from Amazon S3.
     *
     * @param persistableDownload
     *            the download to resume.
     * @return A new <code>Download</code> object to use to check the state of
     *         the download, listen for progress notifications, and otherwise
     *         manage the download.
     *
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in Amazon S3 while processing the
     *             request.
     */
    public Download resumeDownload(PersistableDownload persistableDownload) {
        assertParameterNotNull(persistableDownload,
                "PausedDownload is mandatory to resume a download.");
        GetObjectRequest request = new GetObjectRequest(
                persistableDownload.getBucketName(), persistableDownload.getKey(),
                persistableDownload.getVersionId());
        if (persistableDownload.getRange() != null
                && persistableDownload.getRange().length == 2) {
            long[] range = persistableDownload.getRange();
            request.setRange(range[0], range[1]);
        }
        request.setRequesterPays(persistableDownload.isRequesterPays());
        request.setResponseHeaders(persistableDownload.getResponseHeaders());

        return doDownload(request, new File(persistableDownload.getFile()), null, null,
                APPEND_MODE, 0);
    }

    /**
     * <p>
     * Asserts that the specified parameter value is not <code>null</code> and if it is,
     * throws an <code>IllegalArgumentException</code> with the specified error message.
     * </p>
     *
     * @param parameterValue
     *            The parameter value being checked.
     * @param errorMessage
     *            The error message to include in the IllegalArgumentException
     *            if the specified parameter is null.
     */
    private void assertParameterNotNull(Object parameterValue, String errorMessage) {
        if (parameterValue == null) throw new IllegalArgumentException(errorMessage);
    }

    /**
     * Releasing all resources created by <code>TransferManager</code> before it
     * is being garbage collected.
     */
    @Override
    protected void finalize() throws Throwable {
        shutdown();
    }
}