package evaluation;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import utils.ClientParameters;
import utils.SessionState;
import services.*;
import services.Request.RequestType;
import services.Service.ServiceType;
import utils.Errors;
import utils.MathUtils;
import utils.MathUtils.SummaryStats;


public class PerformanceEvaluationLogger {
	// Amazon S3 cost constants (taken in April 2015 from the AWS website).
		final double putCopyListOpsCost = 0.005 / 1000.0; // 0.005 USD per 1000 requests
		final double deleteOpCost = 0.000; // Free
		final double getOpCost = 0.004 / 10000.0; // 0.004 USD per 10000 requests
		final double storageCostPerGBMonth = 0.0300; // $0.0300 per GB - first 1 TB / month of storage used
		final double dataTransferCostPerGB = 0.090; // $0.090 per GB - first 10 TB / month data transfer out beyond the global free tier
		
		public boolean summarizeOps = true; // false;
		
		private static final PerformanceEvaluationLogger instance = new PerformanceEvaluationLogger();
		
		protected long openCallTime = -1;
		protected long openDoneTime = -1;
		
		protected long closeCallTime = -1;
		protected long closeDoneTime = -1;
		
		protected long connectCallTime = -1;
		protected long connectDoneTime = -1;
		
		protected long disconnectCallTime = -1;
		protected long disconnectDoneTime = -1;
		
		protected long localByteSize = -1;
		protected long storageByteSize = -1;
		
		protected long peakByteSize = -1;
		
		public static PerformanceEvaluationLogger getInstance() { return instance; }
		
		private PerformanceEvaluationLogger() { clear(); }
		
		public void clear()
		{
			openCallTime = -1;
			openDoneTime = -1;
			
			closeCallTime = -1;
			closeDoneTime = -1;
			
			connectCallTime = -1;
			connectDoneTime = -1;
			
			disconnectCallTime = -1;
			disconnectDoneTime = -1;

			requestsMap.clear();
		}
		
		protected Map<Long, RequestLogItem> requestsMap = new HashMap<Long, RequestLogItem>();
		
		private long getCurrentTime() { return System.currentTimeMillis(); }

		public void openCall() 
		{
			assert(openDoneTime == -1); openCallTime = getCurrentTime(); 
			
			scheduleInitRequest(); // schedule init
		}
		public void openDone() 
		{ 
			assert(openCallTime != -1 && openDoneTime == -1 && closeCallTime == -1); 
			openDoneTime = getCurrentTime(); 
			
			completeInitRequest(); // complete init
		}
		
		public void closeCall() { assert(closeCallTime == -1); closeCallTime = getCurrentTime(); }
		public void closeDone() { assert(closeCallTime != -1 && closeDoneTime == -1); closeDoneTime = getCurrentTime(); }
		
		public void connectCall() { connectCallTime = getCurrentTime(); }
		public void connectDone() { connectDoneTime = getCurrentTime(); }
		
		public void disconnectCall() { disconnectCallTime = getCurrentTime(); }
		public void disconnectDone() { disconnectDoneTime = getCurrentTime(); }

		public boolean isOpened() { return (openDoneTime != -1); }
		public boolean isClosed() { return (closeDoneTime != -1); }
		
		public boolean isOpening() { return (openCallTime != -1 && openDoneTime == -1); }

		public void scheduleRequest(Request req) 
		{
			long reqId = req.getId();
			if(requestsMap.containsKey(reqId) != false) { Errors.error("Request " + reqId + " has already been scheduled!"); }
			
			assert(isOpening() == false && isOpened() == true);
			RequestLogItem logItem = new RequestLogItem(req);
			
			if(req.getType() == RequestType.PUT)
			{
				PutRequest put = (PutRequest)req;
				long byteSize = put.getValue().getData().length;
				logItem.setItemByteSize(byteSize);
			}
			logItem.scheduled(); // set the request as scheduled
			requestsMap.put(reqId, logItem);
		}

		public void completeRequest(ScheduledRequest sreq) 
		{
			Request req = sreq.getRequest();
			assert(sreq.isReady() == true); assert(req != null);	
			long reqId = req.getId();
			
			if(requestsMap.containsKey(reqId) == false) { Errors.error("Request " + reqId + " has not been scheduled!"); }
			
			RequestLogItem logItem = requestsMap.get(reqId);
			assert(logItem != null);
			
			if(sreq.wasSuccessful() == false) { logItem.setFailed(); }
			
			if(req.getType() == RequestType.GET)
			{
				long byteSize = 0;
				if(sreq.wasSuccessful() == true) { byteSize = sreq.getDataItem().getData().length; }
				logItem.setItemByteSize(byteSize);
			}
			logItem.completed();
		}
		
		public void scheduleInitRequest()
		{
			if(requestsMap.containsKey(Request.initReqId) != false) { Errors.error("Init Request has already been scheduled!"); }
			
			RequestLogItem logItem = new RequestLogItem(); // init request
			logItem.scheduled();
			requestsMap.put(Request.initReqId, logItem);
		}
		
		public void completeInitRequest()
		{
			if(requestsMap.containsKey(Request.initReqId) == false) { Errors.error("Init Request has not been scheduled!"); }
			
			RequestLogItem logItem = requestsMap.get(Request.initReqId);
			logItem.completed();
		}
		
		public void scheduleOperation(Service op)
		{
			long reqId = op.getRequestId();
			
			if(requestsMap.containsKey(reqId) != true) { Errors.error("Request " + reqId + " has not been scheduled!"); }
			RequestLogItem logItem = requestsMap.get(reqId);
			
			OperationLogItem opLogItem = new OperationLogItem(op);
			opLogItem.scheduled();
			
			if(op.getType() == ServiceType.UPLOAD)
			{
				UploadObject up = (UploadObject)op;
				long byteSize = up.getDataItem().getData().length;
				opLogItem.setObjectByteSize(byteSize);
			}
			
			logItem.AddOperation(opLogItem);
		}
		
		
		public void completeOperation(ScheduledOperation sop)
		{
			Service op = sop.getOperation();
			long reqId = op.getRequestId();
			
			if(requestsMap.containsKey(reqId) != true) { Errors.error("Request " + reqId + " has not been scheduled!"); }
			RequestLogItem logItem = requestsMap.get(reqId);
			
			OperationLogItem opLogItem = logItem.LookupOperation(op.getOperationId());
			
			if(sop.wasSuccessful() == false) { opLogItem.setFailed(); }
			
			if(op.getType() == ServiceType.DOWNLOAD || op.getType() == ServiceType.DELETE)
			{
				long byteSize = 0;
				if(op.getType() == ServiceType.DOWNLOAD)
				{ if(sop.wasSuccessful() == true) {  byteSize = sop.getDataItem().getData().length; } }
				opLogItem.setObjectByteSize(byteSize);
			}
			
			opLogItem.completed();
		}
		
		
		private String getEventString(String eventName, long time1, long time2) 
		{
			double elapsed = (time2 - time1) / 1000.0;
			return "[Event: " + eventName + "] start: " + time1 + ", end: " + time2 + " (elapsed: " + elapsed + " s)";
		}
		
		public void dumpToFile(File logFile)
		{
			//assert(closeDoneTime != -1 && disconnectDoneTime != -1);
			try
			{
				FileWriter fw = new FileWriter(logFile, !SessionState.getInstance().shouldReset());
				BufferedWriter bw = new BufferedWriter(fw);
				
				bw.write(getEventString("open", openCallTime, openDoneTime)); bw.newLine();
				bw.write(getEventString("connect", connectCallTime, connectDoneTime)); bw.newLine();
				bw.write(getEventString("close", closeCallTime, closeDoneTime)); bw.newLine();
				bw.write(getEventString("disconnect", disconnectCallTime, disconnectDoneTime)); bw.newLine();
				bw.newLine();
				
				bw.write("----------------------------------------------------------------------------------------"); bw.newLine();
				
				// exclude the time for close (since there may be a cloning op)
				double totalElapsedSec = (closeDoneTime - openCallTime) / 1000.0; // (closeCallTime - openCallTime) / 1000.0; 
				
				long totalReqCount = 0; long initReqCount = 0; long failedReqCount = 0; long incompleteReqCount = 0;
				long uploadOps = 0; long downloadOps = 0; long copyAndListOps = 0; long deleteOps = 0;
				double elapsedTimePerReq = 0.0; double itemKBPerReq = 0.0; double opsCountPerReq = 0.0;
				double opsDownloadedKBPerReq = 0.0; double opsUploadedKBPerReq = 0.0; double opsCompleteCountPerReq = 0.0;
				List<Double> dOpsCountList = new ArrayList<Double>();
				List<Double> uOpsCountList = new ArrayList<Double>();
				List<Double> opsCountList = new ArrayList<Double>();
				
				for(long reqId : new TreeSet<Long>(requestsMap.keySet()))
				{
					RequestLogItem reqLogItem = requestsMap.get(reqId);
					bw.write(reqLogItem.toString(summarizeOps)); bw.newLine();
					
					if(reqLogItem.isInit() == true)	{ initReqCount++; }
					if(reqLogItem.hasFailed() == true) { failedReqCount++; }
					if(reqLogItem.isComplete() == false) { incompleteReqCount++; }
					
					if(reqLogItem.isInit() == false) 
					{
						elapsedTimePerReq += reqLogItem.getElapsedTime();
						itemKBPerReq += reqLogItem.getItemKB(); 
						
						opsCountPerReq += reqLogItem.getOpsCount();
						opsDownloadedKBPerReq += reqLogItem.getOpsKB(ServiceType.DOWNLOAD);
						opsUploadedKBPerReq += reqLogItem.getOpsKB(ServiceType.UPLOAD);
						opsCompleteCountPerReq += reqLogItem.getOpsCompleteCount();
						
						double dOps = reqLogItem.getOpsCount(ServiceType.DOWNLOAD); dOpsCountList.add(dOps);
						double uOps = reqLogItem.getOpsCount(ServiceType.UPLOAD); uOpsCountList.add(uOps);
						opsCountList.add(dOps + uOps);
					}
					
					downloadOps += reqLogItem.getOpsCount(ServiceType.DOWNLOAD);
					uploadOps += reqLogItem.getOpsCount(ServiceType.UPLOAD);
					
					
					totalReqCount++;
				}
				bw.newLine();
				

				bw.write("----------------------------------------------------------------------------------------"); bw.newLine();
				bw.write("----------------------------------------------------------------------------------------"); bw.newLine();
				bw.newLine();			
				
				SessionState ss = SessionState.getInstance();
				bw.write("Experiment: " + ss.experimentHash + ", client: " + ss.client + ", storage: " + ss.storage); bw.newLine();
				bw.newLine();
				
				double reqThroughput = (totalReqCount - failedReqCount) / totalElapsedSec; double reqPerSecOverall = 1.0 / reqThroughput;
				bw.write("Processed " + totalReqCount + " requests (" + initReqCount + " init, " + incompleteReqCount + " incomplete, "+ failedReqCount
				+ " failed) in " + totalElapsedSec + " seconds (" +  String.format("%.2f", reqThroughput) + " req/sec <-> " + String.format("%.2f", reqPerSecOverall) + " sec/req)!"); bw.newLine(); bw.newLine();
				
				final double bytesPerMB = 1024.0 * 1024.0;
				double localMB = localByteSize / bytesPerMB; 
				double serverMB = storageByteSize / bytesPerMB;
				double peakMB = peakByteSize / bytesPerMB;
				bw.write("Storage: local -> " + String.format("%.1f", localMB) + " MB, peak -> ");
				bw.write(String.format("%.1f", peakMB)+ " MB, external -> ");
				bw.write(String.format("%.1f", serverMB) + " MB"); bw.newLine(); bw.newLine();
				
				double totalOpsCount = opsCountPerReq;
				if(totalReqCount > 0)
				{
					elapsedTimePerReq /= totalReqCount;
					itemKBPerReq /= totalReqCount;
					
					opsCountPerReq /= totalReqCount;
					opsDownloadedKBPerReq /= totalReqCount;
					opsUploadedKBPerReq /= totalReqCount;
					
					opsCompleteCountPerReq /= totalOpsCount;
					
					double elapsedTimePerReqStd = 0.0;
					for(long reqId : new TreeSet<Long>(requestsMap.keySet()))
					{
						RequestLogItem reqLogItem = requestsMap.get(reqId);
						
						elapsedTimePerReqStd += Math.pow((reqLogItem.getElapsedTime() - elapsedTimePerReq), 2.0);
					}
					elapsedTimePerReqStd = Math.sqrt(elapsedTimePerReqStd / totalReqCount);
					
					double parallelFactor = elapsedTimePerReq / reqPerSecOverall;
					bw.write("Elapsed seconds per request: " + String.format("%.3f", elapsedTimePerReq) + " (std: " + String.format("%.3f", elapsedTimePerReqStd) + ") [" + opsCompleteCountPerReq*100.0 + "% ops completed] - parallelization factor: " + String.format("%.2f", parallelFactor)); bw.newLine();
					bw.write("Per request average: " + String.format("%.2f", opsCountPerReq) + " storage ops, " + String.format("%.2f", opsDownloadedKBPerReq) 
							+ " KB downloaded, " + String.format("%.2f", opsUploadedKBPerReq) + " KB uploaded"); bw.newLine(); bw.newLine();
					
					if(dOpsCountList.isEmpty() == false)
					{ 
						SummaryStats dOpsStats = MathUtils.getInstance().summarize(dOpsCountList);
						bw.write("Download ops per request: median: " + (int)dOpsStats.median + ", iqr: " + (int)dOpsStats.iqr + ", 0.95-q: " + (int)dOpsStats.q95 + ", 0.99-q: " + (int)dOpsStats.q99 + ", min: " + (int)dOpsStats.min+ ", max: " + (int)dOpsStats.max); bw.newLine();
					}
					if(uOpsCountList.isEmpty() == false)
					{ 
						SummaryStats uOpsStats = MathUtils.getInstance().summarize(uOpsCountList);
						bw.write("Upload ops per request: median: " + (int)uOpsStats.median + ", iqr: " + (int)uOpsStats.iqr + ", 0.95-q: " + (int)uOpsStats.q95 + ", 0.99-q: " + (int)uOpsStats.q99 + ", min: " + (int)uOpsStats.min+ ", max: " + (int)uOpsStats.max); bw.newLine();
					}
					if(opsCountList.isEmpty() == false)
					{
						SummaryStats opsStats = MathUtils.getInstance().summarize(opsCountList);
						bw.write("Ops per request: median: " + (int)opsStats.median + ", iqr: " + (int)opsStats.iqr + ", 0.95-q: " + (int)opsStats.q95 + ", 0.99-q: " + (int)opsStats.q99 + ", min: " + (int)opsStats.min+ ", max: " + (int)opsStats.max); bw.newLine(); bw.newLine();
					}
					
					double downloadedKB = opsDownloadedKBPerReq * totalReqCount; double downloadBW = downloadedKB / totalElapsedSec;
					double uploadedKB = opsUploadedKBPerReq * totalReqCount; double uploadBW = uploadedKB / totalElapsedSec;
					bw.write("Overall storage: " + String.format("%.2f", downloadedKB/1024.0) + " MB downloaded, " + String.format("%.2f", uploadedKB/1024.0) + " MB uploaded -> " +
							String.format("%.2f", downloadBW) + " KB/s D, " + String.format("%.2f", uploadBW) + " KB/s U"); bw.newLine();
					
					double reqsKB = itemKBPerReq * totalReqCount; double reqsBW = reqsKB / totalElapsedSec;
					bw.write("Overall requests: " + String.format("%.2f", reqsKB / 1024.0) + " MB transfered, "+ String.format("%.2f", reqsBW) + " KB/s"); bw.newLine(); bw.newLine();
				
					{
						double downloadCost = downloadOps * getOpCost;
						double uploadCost = uploadOps * putCopyListOpsCost;
						
						double otherOpsCost = copyAndListOps * putCopyListOpsCost + deleteOps * deleteOpCost;
						
						double storageCost = (serverMB / 1024) * storageCostPerGBMonth;
					
						double dataTransferCost = downloadedKB / 1024 / 1024 * dataTransferCostPerGB;
						
						double totalCostNoStorage = downloadCost + uploadCost + otherOpsCost + dataTransferCost;
						bw.write("Overall S3 cost (no storage): " + String.format("%.5f", totalCostNoStorage) + " USD, D/U/(RM,C,L): " + String.format("%.3f", downloadCost)
								+ "/" + String.format("%.3f", uploadCost) + "/" + String.format("%.3f", otherOpsCost) + ", storage: " + String.format("%.3f", storageCost) + " USD, data transfer (out): " + String.format("%.3f", dataTransferCost)); bw.newLine();
					}
				}
				
				bw.newLine(); bw.newLine();
				bw.flush();
				bw.close();
			}
			catch(Exception e) { Errors.error(e); }
		}

		public void setLocalByteSize(long bytes) { localByteSize = bytes; }
		public void setStorageByteSize(long bytes) { storageByteSize = bytes; }

		public void setPeakByteSize(long bytes) { peakByteSize = bytes; }
		
		public void summarizeToFile(File perfSummaryFile, String runner) 
		{
			SessionState ss = SessionState.getInstance();
			ClientParameters clientParams = ClientParameters.getInstance();
			
			boolean writeHeader = !perfSummaryFile.exists();
			
			try
			{
				FileWriter fw = new FileWriter(perfSummaryFile, true);
				BufferedWriter bw = new BufferedWriter(fw);
				
				String header = "";
				
				String line = "";
				line += ss.experimentHash + ", " + ss.client + ", " + ss.storage + ", " + runner + ", " + ss.fastInit + ", ";
				header += "exp hash, client, storage, runner, fast init, ";
				
				line += clientParams.maxBlocks + ", " + clientParams.contentByteSize + ", ";// + clientParams.localPosMapCutoff + ", ";
				header += "N, l, ";
				
				final double bytesKB = 1024.0; final double KBMB = 1024.0;
				final double bytesMB = bytesKB * KBMB; final double MBGB = 1024.0;
				double oramSizeMB = clientParams.contentByteSize * clientParams.maxBlocks / bytesMB;
				double localMB = localByteSize / bytesMB; 
				double serverMB = storageByteSize / bytesMB;
				double peakMB = peakByteSize / bytesMB;
				
				line += String.format("%.3f", oramSizeMB) + ", " + String.format("%.3f", localMB) + ", " + String.format("%.3f", peakMB) + ", " + (int)serverMB + ", ";
				header += "capacity (MB), local (MB), peak (MB), server (MB), ";
				
				double totalElapsedTime = (closeDoneTime - openCallTime) / 1000.0;
				double openElapsedTime = (openDoneTime - openCallTime) / 1000.0;
				double closeElapsedTime = (closeDoneTime - closeCallTime) / 1000.0;
				
				long reqs = 0; double totalUsefulKB = 0.0; 			
				double meanReqSecPerOp = 0.0;
				
				double meanReqResponseTime = 0.0;  double stdReqResponseTime = 0.0;
				double minReqResponseTime = -1.0; double maxReqResponseTime = 0.0;
				
				long dOps = 0; long uOps = 0; double minOpsPerReq = -1; double maxOpsPerReq = 0;
				double totalDLMB = 0.0; double totalULMB = 0.0;
				
				double meanDOpsTime = 0.0; double meanUOpsTime = 0.0;
				
				RequestLogItem initReq = null;
				for(long reqId : requestsMap.keySet())
				{
					RequestLogItem reqLogItem = requestsMap.get(reqId);				
					if(reqLogItem.isInit() == true)	{ initReq = reqLogItem; continue; }
					
					totalUsefulKB += reqLogItem.getItemKB();
					
					double elapsedTime = reqLogItem.getElapsedTime();
					meanReqResponseTime += elapsedTime;
					
					if(minReqResponseTime == -1.0 || elapsedTime < minReqResponseTime) { minReqResponseTime = elapsedTime; }
					if(elapsedTime > maxReqResponseTime) { maxReqResponseTime = elapsedTime; }
					
					int d = reqLogItem.getOpsCount(ServiceType.DOWNLOAD); dOps += d;
					int u = reqLogItem.getOpsCount(ServiceType.UPLOAD); uOps += u;
					int ops = d + u;
					
					totalDLMB += reqLogItem.getOpsKB(ServiceType.DOWNLOAD) / KBMB;
					totalULMB += reqLogItem.getOpsKB(ServiceType.UPLOAD) / KBMB;
					
					if(minOpsPerReq == -1 || ops < minOpsPerReq) { minOpsPerReq = ops; } 
					if(ops > maxOpsPerReq) { maxOpsPerReq = ops; }
					
					meanReqSecPerOp += (ops > 0.0) ? (elapsedTime / ops) : 0.0; 
					
					List<Double> dTimes = reqLogItem.getOpsTimes(ServiceType.DOWNLOAD);
					double meanDOpsTmp = 0;
					for(double time : dTimes) { meanDOpsTmp += time; }
					meanDOpsTime += (dTimes.size() > 0) ? (meanDOpsTmp / dTimes.size()) : 0;
					
					List<Double> uTimes = reqLogItem.getOpsTimes(ServiceType.UPLOAD);
					double meanUOpsTmp = 0;
					for(double time : uTimes) { meanUOpsTmp += time; }
					meanUOpsTime += (uTimes.size() > 0) ? (meanUOpsTmp / uTimes.size()) : 0;
					
					reqs++;
				}
				meanReqResponseTime /= reqs;
				meanReqSecPerOp /= reqs;
				
				meanDOpsTime /= reqs; meanUOpsTime /= reqs;
				
				// don't remove it, since it's logged also and thus we can remove it later, if needed
				//totalElapsedTime -= initReq.getElapsedTime(); // remove init time
				
				long totalOps = dOps + uOps;
				double meanOpsPerReq = (double)totalOps / reqs; double stdOpsPerReq = 0.0;
				
				long copyAndListOps = 0; long deleteOps = 0;
				for(long reqId : requestsMap.keySet())
				{
					RequestLogItem reqLogItem = requestsMap.get(reqId);				
					if(reqLogItem.isInit() == true)	{ continue; }
					
					double elapsedTime = reqLogItem.getElapsedTime();
					
					stdReqResponseTime += Math.pow((meanReqResponseTime - elapsedTime), 2);
					
					int d = reqLogItem.getOpsCount(ServiceType.DOWNLOAD); 
					int u = reqLogItem.getOpsCount(ServiceType.UPLOAD); 
					int ops = d + u;
					
					
					int rm = reqLogItem.getOpsCount(ServiceType.DELETE);
					deleteOps += rm;
				
					stdOpsPerReq += Math.pow((ops - meanOpsPerReq), 2.0);
				}
				stdReqResponseTime = Math.sqrt(stdReqResponseTime / reqs);
				
				stdOpsPerReq = Math.sqrt(stdOpsPerReq / reqs);
				
				double seqReqThroughput = reqs / totalElapsedTime;
				double meanReqThroughput = 1.0 / meanReqResponseTime;
				
				double parallelizationFactor = meanReqThroughput / seqReqThroughput;
				
				double throughputKB = totalUsefulKB / totalElapsedTime;
				
				// S3 cost
				double totalCostNoStorage = 0.0; double storageCost = 0.0;
				{
					double downloadCost = dOps * getOpCost;
					double uploadCost = uOps * putCopyListOpsCost;
					
					double otherOpsCost = copyAndListOps * putCopyListOpsCost + deleteOps * deleteOpCost;
					
					storageCost = (serverMB / MBGB) * storageCostPerGBMonth;
				
					double dataTransferCost = (totalDLMB / MBGB) * dataTransferCostPerGB;
					
					totalCostNoStorage = downloadCost + uploadCost + otherOpsCost + dataTransferCost;
				}
				
				// now req stuff
				line += reqs + ", " + String.format("%.2f", totalElapsedTime) + ", " + String.format("%.2f", throughputKB) + ", ";
				line += String.format("%.2f", seqReqThroughput) + ", " + String.format("%.3f", meanReqSecPerOp) + ", " + String.format("%.3f", meanReqThroughput) +", ";
				line += String.format("%.3f", meanReqResponseTime) + ", " + String.format("%.3f", stdReqResponseTime) + ", ";
				line += String.format("%.3f", minReqResponseTime) + ", " + String.format("%.3f", maxReqResponseTime) + ", ";
				line += String.format("%.2f", parallelizationFactor) + ", ";
				
				line += String.format("%.2f", initReq.getElapsedTime()) + ", " + String.format("%.2f", openElapsedTime) + ", " + String.format("%.2f", closeElapsedTime) + ", " + initReq.getOpsCompleteCount() + ", ";
				
				
				header += "reqs, elapsed (s), tp (KB/s), ";
				header += "seq tp (req/s), per op req (s/ops), mean tp (req/s), ";
				header += "mean rt (s), std rt (s), ";
				header += "min rt (s), max rt (s), ";
				header += "p-factor, ";
				
				header += "init elapsed (s), open time (s), close time (s), init (ops), ";
				
				// now ops stuff
				line += dOps + ", " + uOps + ", ";
				line += String.format("%.3f", meanOpsPerReq) + ", " + String.format("%.3f", stdOpsPerReq) + ", ";
				line += (int)minOpsPerReq + ", " + (int)maxOpsPerReq + ", ";
				line += String.format("%.2f", totalDLMB) + ", " + String.format("%.2f", totalULMB) + ", ";
				line += String.format("%.2f", meanDOpsTime) + ", " + String.format("%.2f", meanUOpsTime) + ", ";
				
				header += "d (ops), u (ops), ";
				header += "mean (ops/req), std (ops/req), ";
				header += "min (ops/req), max (ops/req), ";
				header += "total d (MB), total u (MB), ";
				header += "mean d ops time (ms), mean u ops time (ms), ";
				
				
				line += String.format("%.4f", storageCost) + ", " + String.format("%.4f", totalCostNoStorage);
				
				header += "storage cost (USD/month), remaining total cost (USD)";
				
				
				if(writeHeader == true) { bw.write(header); bw.newLine(); }
				bw.write(line); bw.newLine();
				
				bw.flush();
				bw.close();
			}
			catch(Exception e) { Errors.error(e); }
		}
}
