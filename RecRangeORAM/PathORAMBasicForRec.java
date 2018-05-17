package RecRangeORAM;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import BatchORAM.PathORAMBasicBatchEvict.Tree;
import BatchORAM.PathORAMBasicBatchEvict.Tree.Block;
import Interfaces.ExternalStorageInterface;
import backend.LocalStorage;
import data.DataItem;
import data.SimpleDataItem;
import pollable.Pollable;
import services.DownloadObject;
import services.PutRequest;
import services.Request;
import services.ScheduledOperation;
import services.UploadObject;
import services.Request.RequestType;
import utils.ClientParameters;
import utils.Errors;
import utils.Log;
import utils.SessionState;

public class PathORAMBasicForRec {

	
protected Log log = Log.getInstance();
	
  protected SecureRandom rnd;
	int dataSize; // measured in byte.
	int extDataSize; // measured in byte.
	static final int keyLen = 10; // length of key (to encrypt each data piece) in bytes
	static final int nonceLen = 10; // length of nonce (to encrypt each data piece) in bytes

	public static int Z = 4;
	public static int stashSize = 200; // set according to table 3 of PathORAM paper (for 80-bit security)
	
//	public static int C = 1024;
	public static int C = 4; // memory reduction factor (ensure it is a power of 2)
	
	public static int bucketLimitPerFile = (int) Math.pow(2, 16);
	
	public static final boolean stashUseLS = false;
	//public static final boolean stashUseLS = true;
	
	public Tree serverTree;
	int recLevel;
	
	byte[] clientKey;
	
	
	

	
	
	public class Tree {
		public int N; // the number of logic blocks in the tree
		public int D; // depth of the tree
//		int dataLen; // data length in bits
		public int a;
		ExternalStorageInterface storedTree;
		long treeSize;
		public Stash stash;
		public BitSet[] posMap;
		int rangeSize;
		long evictCounter;
		Tree() { }
		int oramID;
		int totalNumberOfORAMs;
		long uploadBytes = 0;
		long downloadBytes = 0;
		long seeks = 0;
		/*
		 * Given input "data" (to be outsourced), initialize the server side storage and return the client side position map. 
		 * No recursion on the tree is considered. 
		 */
		

	    Logger upLog = Logger.getLogger("upLog");
	    Logger downLog = Logger.getLogger("downLog");
	    Logger seekLog = Logger.getLogger("seekLog");			
		
	    FileHandler fhUp;
	    FileHandler fhDown;
	    FileHandler fhSeek;
		public int getRevLexLabel(int originalLabel, int depth) {
		
			if(depth == 0)
				return originalLabel;
			
			int temp = (int) (originalLabel) - (int) (2*Math.pow(2, depth-1)-1);
			boolean arr[] = new boolean[(int)(Math.log(Math.pow(2, depth))/Math.log(2))];
			
			for (int i =  (int) (Math.log(Math.pow(2,depth))/Math.log(2))-1; i >= 0; i--){
				if(temp%2 == 0)
					arr[i] = true;
				else
					arr[i] = false;
				temp = temp >> 1;
			}
			int revLexLabel = 0;
			for(int i = 0; i < arr.length; i++){
				if(!arr[i]){
					revLexLabel += Math.pow(2, i);
		
				}
			}
		
			return revLexLabel;
		}

		
		int getDepth(int index) {
			
			if(index == 0)
				return 0;
			
			int depth = 1;
			while(index >= Math.pow(2, depth)-1) {
				depth++;
			}
			
			return depth-1;
		}
		
		private BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int dSize, BitSet[] data, int rangeSize, int oramID, ArrayList<List<Integer>> permutationList) 
		{
			storedTree = si;
			dataSize = dSize;
			this.totalNumberOfORAMs = permutationList.size();

			extDataSize = dataSize + 4 + 4+8 + totalNumberOfORAMs*4;
			this.rangeSize = rangeSize;
			this.oramID = oramID;
			this.a = 8;
			// generate a random permutation for init, so we don't get a silly stash overflow
		//	List<Integer> permutation = new ArrayList<Integer>();
         //   for (int i = 0; i < maxBlocks/rangeSize; i++) { permutation.add(i); }
          //  Collections.shuffle(permutation);
            
			// TODO
			
			
			evictCounter = 0;
          /* 
 			BitSet[] posMap = new BitSet[(N/rangeSize + C-1) / C];	// clientPosMap[i] is the leaf label of the i-th block.
 			for (int i = 0; i < posMap.length; i++)	{ posMap[i] = new BitSet(C*D); }

 			for (int i = 0; i < N/rangeSize; i++) 
 			{
 				int p = i;
 				if(i < permutation.size()) { p = permutation.get(i); }
 				Utils.writePositionMap(posMap, this, i, p); 
 				
 				//{ log.append("[POB (initialize)] Block " + i + " -> leaf " + p, Log.TRACE); }
 			}
 			*/
			



		    try {  

		        // This block configure the logger with handler and formatter  
		        fhUp = new FileHandler("/home/anrin/ORAM/code/concurORAM/performance_logs/upload_logs/ORAM_" + this.oramID+"_upload.log");
		        fhDown = new FileHandler("/home/anrin/ORAM/code/concurORAM/performance_logs/download_logs/ORAM_" + this.oramID+"_download.log");
		        fhSeek = new FileHandler("/home/anrin/ORAM/code/concurORAM/performance_logs/seek_logs/ORAM_" +"seek.log");
		        upLog.addHandler(fhUp);
		        downLog.addHandler(fhDown);
		        seekLog.addHandler(fhSeek);
		        
		        SimpleFormatter formatter = new SimpleFormatter();  
		        fhUp.setFormatter(formatter);  
		        fhDown.setFormatter(formatter);
		        fhSeek.setFormatter(formatter);
		        // the following statement is used to log any messages  
		       // downLog.info("My first log");  

		    } catch (SecurityException e) {  
		        e.printStackTrace();  
		    } catch (IOException e) {  
		       e.printStackTrace();  
		    }  

		    //logger.info("Hi How r u?");  

			
 			this.posMap = buildTree(maxBlocks, data, oramID, permutationList);
			stash = new Stash(stashSize, recLevel, stashUseLS);
 			
			return this.posMap;
		}

		private BitSet[] buildTree(int maxBlocks,  BitSet[] dataArray, int oramID, ArrayList<List<Integer>> permutationList) 
		{
//			storedTree = new LocalStorage();
//			storedTree.initialize(null, "/tmp/Cloud" + recLevel);
			
			SessionState ss = SessionState.getInstance();
			Map<String, Request> fastInitMap = ss.fastInitMap;
			if(ss.fastInit == false) {  fastInitMap = null; }
			
			// set N to be the smallest power of 2	it that is bigger than 'data.length'. 
			N = (int) Math.pow(2, Math.ceil(Math.log(maxBlocks)/Math.log(2)));
			D = Utils.bitLength(N)-1;
			
			BitSet[] posMap = new BitSet[((N/rangeSize) + C-1) / C];	// clientPosMap[i] is the leaf label of the i-th block.
 			for (int i = 0; i < posMap.length; i++)	{ posMap[i] = new BitSet(C*D); }

			
			
			final int removeIntervalSize = 512; final double sizeFactorForSlowdown = 0.75;
			final int logIntervalSize = 512;
			Vector<Pollable> v = new Vector<Pollable>();
			Bucket temp;
			// initialize the tree
			treeSize = 2*N-1;
			
			
			for (int i = 0; i < treeSize; i++) 
			{
				
				if (i < treeSize/2) { temp = new Bucket(new Block()); }
				else {
					if (i-N+1 < maxBlocks)
					{
					
										
						int rangeLabel = Math.floorDiv(i-N+1, rangeSize);
						int rangeOffset = (i-N+1)%rangeSize;
						int id = permutationList.get(this.oramID).indexOf(rangeLabel)*rangeSize+ rangeOffset ;
		 				if(id%(rangeSize) == 0)
		 					Utils.writePositionMap(posMap, this, id/rangeSize, (i-N+1)); 
		
		 				
						
						
						BitSet data = null;
						
						String blockIdStr = "" + id;
						if(recLevel == 0 && fastInitMap != null && fastInitMap.containsKey(blockIdStr) == true)
						{
							Request req = fastInitMap.get(blockIdStr);
							Errors.verify(req.getType() == RequestType.PUT);
							PutRequest put = (PutRequest)req;
							byte[] val = put.getValue().getData();
							Errors.verify(ClientParameters.getInstance().contentByteSize <= dataSize);
							Errors.verify(val.length <= dataSize);
							
							data = BitSet.valueOf(val);
						}
						else 
						{
							if(dataArray != null)
							{
								Errors.verify(dataArray.length > id);
								data = dataArray[id];
							}
							else
							{
								long[] val = new long[1]; val[0] = id;
								data = BitSet.valueOf(val);
							}
						}
						
						Block blk = new Block(data, id, (i-N+1));
						
						for(int k = 0; k < permutationList.size(); k++) {
							int rSize = (int) Math.pow(2, k);
							int rLabel = Math.floorDiv(i-N+1, rSize);
							int rOffset = (i-N+1)%rSize;
							int rID = permutationList.get(k).indexOf(rLabel)*rSize+ rOffset; 
							blk.crmData[k] = rID;
						}
						temp = new Bucket(blk);
								
						
						//{ System.out.println("[PathORAMBasic (BuildTree)] (R" + recLevel +") putting block " + id + " to label " + (i-N+1) + " (objectKey: " + recLevel + "depth" + getDepth(i) + "#" + (i-N+1)); }
					}
					else
						temp = new Bucket(new Block());
				}
				
				temp.encryptBlocks();
				
		//		String objectKey = recLevel + "ORAM #" + oramID + "depth" + getDepth(i) + "#"+ (i- ((int) Math.pow(2,getDepth(i))-1)); //getRevLexLabel(i,getDepth(i));
			//	System.out.println(objectKey);
			//	String objectKey = Integer.toString(oramID)+"#" + recLevel;
		
				DataItem di = new SimpleDataItem(temp.toByteArray());
				int fileLabel = Math.floorDiv(i,bucketLimitPerFile);
				String objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
				int fileOffset = i%(bucketLimitPerFile);
			//	System.out.println("Writing --" + objectKey);
				UploadObject upload = new UploadObject(Request.initReqId, objectKey, di);
				upload.setObjectOffset(fileOffset*Z*(extDataSize + nonceLen));
				upload.setObjectSize(Z*(extDataSize + nonceLen));
				ScheduledOperation sop = storedTree.uploadObject(upload);
				
				v.add(sop);
				
				if(i > 0 && (i % removeIntervalSize) == 0) 
				{
					Pollable.removeCompleted(v);
					
					if(v.size() >= (int)(removeIntervalSize * sizeFactorForSlowdown))
					{
						{ log.append("[PathORAMBasic (BuildTree)] Slowing down so storage can catch up...", Log.TRACE); }
						
						int factor = (int)Math.ceil(v.size() / removeIntervalSize); if(factor > 5) { factor = 5; }
						try { Thread.sleep(factor * 5); } catch (InterruptedException e) { Errors.error(e); }
						Pollable.removeCompleted(v);
					}
				}
				
				if(i > 0 && (i % logIntervalSize) == 0)
				{
				//	Log.getInstance().append("[PathORAMBasic (BuildTree)] Uploaded " + (i - v.size()) + " nodes so far.", Log.TRACE);
				}
			}
			
			// This waitForCompletion ensures can be used asynchronously!
			Pollable.waitForCompletion(v);
			//System.exit(0);
			
			return posMap;
		}

		
		
		
		private ArrayList<Tree.Block> downloadChunk(long reqId, int startingBucket[], int numOfBuckets[]) {

		//	System.out.println("fetch Start");
			
			List<HashMap<Integer, Tree.Bucket>> bucketList = new ArrayList<HashMap<Integer, Tree.Bucket>>();

	
			//String objectKey = "ORAMFile";
	//		String objectKey = Integer.toString(oramID)+"#" + recLevel;

			//		System.out.println(objectKey);
			DownloadObject download; 
			String objectKey;
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();

			int[] bucketsUptoEnd = new int[D+1]; 
			int[] bucketsFromStart = new int[D+1]; 
			ArrayList<Integer> pollableList = new ArrayList<Integer>();
			int ctr = 0;
			for(int i=D; i >= 0; i--){
				bucketsUptoEnd[i] = 0;
				bucketsFromStart[i] = 0;
				int startingOffsetAtLevel;
				
				if(i == 0) 
					startingOffsetAtLevel = 0;
				else 
					startingOffsetAtLevel = (int) (Math.pow(2, i)-1);
			
			
		
					int t = 0;
			
					while(t < numOfBuckets[i]) {
						if((startingBucket[i]+t) < ((int) (Math.pow(2, i))))
							bucketsUptoEnd[i] += 1;
						else
							bucketsFromStart[i] += 1;
						t++;
					}
			
					
					int temp = 0;
					int fileLabel = Math.floorDiv(startingBucket[i]+startingOffsetAtLevel,bucketLimitPerFile);
					int fileOffset = (startingBucket[i]+startingOffsetAtLevel)%(bucketLimitPerFile);
					int cntr = 0;

					while(temp < bucketsUptoEnd[i]) {
												
						if(temp + fileOffset >= bucketLimitPerFile) {
											
							objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
							System.out.println("Reading from " + objectKey);
							download = new DownloadObject(reqId, objectKey);
							download.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
							download.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
							ScheduledOperation sop = storedTree.downloadObject(download);
							pollableList.add(cntr);
							fileOffset = 0;
							fileLabel += 1;
							cntr = 0;
							
							v.add(sop);
						}
					temp++;	
					cntr++;
					}
		
					objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
				//	System.out.println("Reading from " + objectKey);
					download = new DownloadObject(reqId, objectKey);
					download.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
					download.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
					ScheduledOperation sop = storedTree.downloadObject(download);
					pollableList.add(cntr);
					v.add(sop);
					
					
					temp = 0;
					fileLabel = Math.floorDiv(startingOffsetAtLevel,bucketLimitPerFile);
					fileOffset = startingOffsetAtLevel%(bucketLimitPerFile);
					cntr = 0;
					
					
					if(bucketsFromStart[i] > 0) {
						
						while(temp < bucketsFromStart[i]) {
							if(temp + fileOffset >= bucketLimitPerFile) {
								objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
								download = new DownloadObject(reqId, objectKey);

								download.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
								download.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
								sop = storedTree.downloadObject(download);
								pollableList.add(cntr);
								fileOffset = 0;
								fileLabel += 1;
								cntr = 0;
						
								v.add(sop);
						}
						temp++;
						cntr++;
					}
					
					
					objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
				//	System.out.println("Reading from " + objectKey);
					download = new DownloadObject(reqId, objectKey);
					download.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
					download.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
					sop = storedTree.downloadObject(download);
					pollableList.add(cntr);
					v.add(sop);	
					}

					/*
					download.setObjectOffset((startingBucket[i]+startingOffsetAtLevel)*Z*(extDataSize + nonceLen));
					download.setObjectSize(Z*(extDataSize + nonceLen)*bucketsUptoEnd[i]);
					ScheduledOperation sop = storedTree.downloadObject(download);
					v.add(sop);
				
					if(bucketsFromStart[i] > 0) {
						download.setObjectOffset(startingOffsetAtLevel*Z*(extDataSize + nonceLen));
						download.setObjectSize(Z*(extDataSize + nonceLen)*bucketsFromStart[i]);
						sop = storedTree.downloadObject(download);
						v.add(sop);
					}
			*/
				}
				
			
		
			
			Pollable.waitForCompletion(v);
		//	System.out.println("FetchEnds");

			
			ArrayList<Tree.Block> blockList = new ArrayList<Tree.Block>();
			
			for(int k = 0; k < pollableList.size(); k++) {
			
			
				byte[] b = v.get(k).getDataItem().getData();
				byte[] bucket = new byte[Z*(extDataSize+nonceLen)];
			
		
			//	for(int j = 0; j < (bucketsUptoEnd[k]); j++) {
				
				for(int j = 0; j < pollableList.get(k); j++) {
					for(int i = 0; i < Z*(extDataSize+nonceLen); i++) { 
						bucket[i] = b[j*(Z*(extDataSize+nonceLen))+i];
							
				}
					Tree.Bucket bkt = new Tree.Bucket(bucket);
					for(Tree.Block blk : bkt.blocks)
						blockList.add(blk);
					
				}	
			}
		
				/*
				if(bucketsFromStart[k] > 0) {
					b = v.get(ctr++).getDataItem().getData();
					for(int j = 0; j < (bucketsFromStart[k]); j++) {
						for(int i = 0; i < Z*(extDataSize+nonceLen); i++) {
							bucket[i] = b[j*(Z*(extDataSize+nonceLen))+i];
					}
			
				
					Tree.Bucket bkt = new Tree.Bucket(bucket);
					for(Tree.Block blk : bkt.blocks)
						blockList.add(blk);
					}	
				}
				*/	
				
		//	}
			
			//seekLog.info("ORAM: " + oramID + "Seeks: " + v.size());
			//downLog.info("ORAM: " + oramID + "Download: " + (blockList.size()*(extDataSize+nonceLen))+ "bytes");
			
			downloadBytes += (blockList.size()*(extDataSize+nonceLen));
			seeks += v.size();
			return  blockList;
			
			}
	
	
	
		
		private void uploadChunk(long reqId, int startingBucket[], int numOfBuckets[], ArrayList<Tree.Block> blockList) {
			
			//String objectKey = "ORAMFile";
		//	String objectKey = Integer.toString(oramID)+"#" + recLevel;
		
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
			String objectKey;
			UploadObject upload;
			
			int startingOffsetAtLevel;
			int nextBlock = 0;
			
			for(int level = D; level >= 0; level--) {
				
				if(level == 0) {
					startingOffsetAtLevel = 0;
				}
				else {
					startingOffsetAtLevel = (int) (Math.pow(2, level)-1);
				}
			
				int bucketsUptoEnd = 0;
				int bucketsFromStart= 0; 
			
				int t = 0;
			
				while(t < numOfBuckets[level]) {
					if((startingBucket[level]+t) < ((int) (Math.pow(2, level))))
						bucketsUptoEnd += 1;
					else
						bucketsFromStart += 1;
					t++;
				}
			
		
	//			byte[] b = new byte[bucketsUptoEnd*(extDataSize+nonceLen)*Z];
		

			//	int k = 0;
			
				//for(int i = 0; i < Z*bucketsUptoEnd; i++) { 
					//byte[] blk = blockList.get(nextBlock++).toByteArray();
					//for(int j = 0; j < (extDataSize+nonceLen); j++) 
						//b[k++] = blk[j];
				//}		
				
				
				int temp = 0;
				int fileLabel = Math.floorDiv(startingBucket[level]+startingOffsetAtLevel,bucketLimitPerFile);
				int fileOffset = (startingBucket[level]+startingOffsetAtLevel)%(bucketLimitPerFile);
				int cntr = 0;
				
				while(temp < bucketsUptoEnd) {
											
					if(temp + fileOffset >= bucketLimitPerFile) { 
						
						
						byte[] b = new byte[cntr*(extDataSize+nonceLen)*Z];
		
						int k = 0;
					
						for(int i = 0; i < Z*cntr; i++) { 
							byte[] blk = blockList.get(nextBlock++).toByteArray();
							for(int j = 0; j < (extDataSize+nonceLen); j++) 
								b[k++] = blk[j];
						}		
			
						objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
						upload = new UploadObject(reqId, objectKey,new SimpleDataItem(b));
						upload.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
						upload.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
						ScheduledOperation sop = storedTree.uploadObject(upload);
						fileOffset = 0;
						fileLabel += 1;
						cntr=0;
						v.add(sop);
					}
					temp++;
					cntr++;
				}
	
				
				byte[] b = new byte[cntr*(extDataSize+nonceLen)*Z];
				
			
				for(int i = 0, k = 0; i < Z*cntr; i++) { 
					byte[] blk = blockList.get(nextBlock++).toByteArray();
					for(int j = 0; j < (extDataSize+nonceLen); j++) 
						b[k++] = blk[j];
				}

				
	
				objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
				upload = new UploadObject(reqId, objectKey,new SimpleDataItem(b));
				upload.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
				upload.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
				ScheduledOperation sop = storedTree.uploadObject(upload);
				v.add(sop);
				
				
				if(bucketsFromStart > 0) {
					temp = 0;
					fileLabel = Math.floorDiv(startingOffsetAtLevel,bucketLimitPerFile);
					fileOffset = (startingOffsetAtLevel)%(bucketLimitPerFile);
					cntr = 0;
					while(temp < bucketsFromStart) {
												
						if(temp + fileOffset >= bucketLimitPerFile) {
							
		
							b = new byte[cntr*(extDataSize+nonceLen)*Z];
						
							int k = 0;
			
							for(int i = 0; i < Z*cntr; i++) { 
								byte[] blk = blockList.get(nextBlock++).toByteArray();
								for(int j = 0; j < (extDataSize+nonceLen); j++) 
									b[k++] = blk[j];
							}
						
							objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
							upload = new UploadObject(reqId, objectKey, new SimpleDataItem(b));
							upload.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
							upload.setObjectSize(Z*(extDataSize + nonceLen)*temp);
							sop = storedTree.uploadObject(upload);
							fileOffset = 0;
							fileLabel += 1;
							cntr = 0;
							v.add(sop);
					}
					cntr++;
					temp++;
					}
			
					
					b = new byte[cntr*(extDataSize+nonceLen)*Z];
					
					
	
					for(int i = 0,k=0; i < Z*cntr; i++) { 
						byte[] blk = blockList.get(nextBlock++).toByteArray();
						for(int j = 0; j < (extDataSize+nonceLen); j++) 
							b[k++] = blk[j];
					}
	
					
					
					objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
					upload = new UploadObject(reqId, objectKey,new SimpleDataItem(b));
					upload.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
					upload.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
					sop = storedTree.uploadObject(upload);
					v.add(sop);

				}
				
				
				
				/*
				UploadObject upload = new UploadObject(reqId, objectKey, new SimpleDataItem(b));
				upload.setObjectOffset((startingOffsetAtLevel+startingBucket[level])*Z*(extDataSize + nonceLen));
				upload.setObjectSize(Z*(extDataSize + nonceLen)*bucketsUptoEnd);
		//	System.out.println("objectOffset at level " + level + "is" + upload.getObjectOffset());
				ScheduledOperation sop = storedTree.uploadObject(upload);
				v.add(sop);
			
			//Pollable.waitForCompletion(v);
			
			//v.clear();
			
				if(bucketsFromStart > 0) {
					b = new byte[bucketsFromStart*(extDataSize+nonceLen)*Z];
					k = 0;
				
				
				
					for(int i = 0; i < Z*bucketsFromStart; i++) { 
						
						byte[] blk = blockList.get(nextBlock++).toByteArray();
						for(int j = 0; j < (extDataSize+nonceLen); j++) 
							b[k++] = blk[j];
					}		
					
					upload = new UploadObject(reqId, objectKey, new SimpleDataItem(b));
					upload.setObjectOffset(startingOffsetAtLevel*Z*(extDataSize + nonceLen));
					upload.setObjectSize((extDataSize + nonceLen)*bucketsFromStart*Z);
					sop = storedTree.uploadObject(upload);
					v.add(sop);
				
						
				}
			*/	
			}
			
			Pollable.waitForCompletion(v);
			
			//seekLog.info("ORAM: " + oramID + "Seeks: " + v.size());
			//upLog.info("ORAM: " + oramID + "Upload: " + (blockList.size()*(extDataSize+nonceLen))+ "bytes");

			uploadBytes += (blockList.size()*(extDataSize+nonceLen));
			seeks += v.size();
			
	//		System.out.println("writing " + (bucketsUptoEnd+bucketsFromStart) + "to level " + level);

		}
		
		
		private ArrayList<Tree.Block> batchReadPaths(long reqId, int leaf,int batchSize) 
		{
		//	List<HashMap<Integer, Tree.Bucket>> bucketList = new ArrayList<HashMap<Integer, Tree.Bucket>>();
			
			int[] numberOfBuckets = new int[D+1];
			int[] bucketLabel = new int[D+1];
			
			for (int i = D; i >= 0; i--) 
			{
				if(i == 0)
					bucketLabel[i] = 0;
				else	
					bucketLabel[i] = (leaf)%((int) Math.pow(2, i));
				numberOfBuckets[i] = ((int) Math.min(Math.pow(2, i),batchSize));
				
			
			}

			return downloadChunk(reqId, bucketLabel, numberOfBuckets);
		}

		
		
		

		public Block[] batchReadRange(long reqId, int leafLabel, int a, int rangeSize) {

			
			
			Tree tr = serverTree;
			Block[] stash = getStash(reqId,tr);
			Block[] result = new Tree.Block[rangeSize];
			
			Arrays.fill(result, null);
			for(Block blk : stash) {
				if (blk.r != null) // when r == null, the block comes from the stash, already decrypted.
				{ blk.dec(); }
				
			}
		

		//	List<HashMap<Integer, Bucket>> bucketList = batchReadPaths(reqId, leafLabel, rangeSize);	
		
			ArrayList<Tree.Block> blockList = batchReadPaths(reqId, leafLabel, rangeSize);
			ArrayList<Integer> dupList = new ArrayList<Integer>();
						
			
			int[] timeStamps = new int[rangeSize];
			Arrays.fill(timeStamps, -1);
			
			for(int i = 0; i < blockList.size(); i ++) {
				if(blockList.get(i).r != null)
					blockList.get(i).dec();
				
				for(int j = 0; j < rangeSize; j++) {
				
					if (blockList.get(i).id == a+j) {
						
						blockList.get(i).duplicate = 1;
						
						if(blockList.get(i).timeStamp > timeStamps[j]) {
							result[j] = new Block(blockList.get(i));
							timeStamps[j] = blockList.get(i).timeStamp;
							result[j].timeStamp = ((int) evictCounter+1);
						//	System.out.println("Found" + (a+j));
						}
					}	
			
				}
					blockList.get(i).enc();
			}
			
			

			for(int j = 0; j < rangeSize; j++) {
				for(Tree.Block blk: stash) {
					if(blk.id != tr.N) {	
						if(blk.id == (a+j) && (blk.timeStamp > timeStamps[j])) {
							result[j] = tr.new Block(blk);
							timeStamps[j] = blk.timeStamp;

					}
				}
				}
				result[j].timeStamp = (int) evictCounter;
			}
			
			
			int[] numberOfBuckets = new int[D+1];
			int[] bucketLabel = new int[D+1];
			
			
			for (int i = D; i >= 0; i--) 
			{
				if(i == 0)
					bucketLabel[i] = 0;
				else	
					bucketLabel[i] = (leafLabel)%((int) Math.pow(2, i));
				numberOfBuckets[i] = ((int) Math.min(Math.pow(2, i),rangeSize));
				
			
			}
			//System.out.println(tr.D);
			//System.out.println("Size of blockList "+blockList.size());
			uploadChunk(reqId, bucketLabel, numberOfBuckets, blockList);
			
			
		
			
			return result;
		}

		


		public class Block {
			int duplicate = -1;
			public BitSet data;
			int id; // range: 0...N-1;
			public int treeLabel;
			int timeStamp;
			public int[] crmData;
			private byte[] r; 
			
			public Block(Block blk) {
				assert (blk.data != null) : "no BitSet data pointers is allowed to be null.";
				try { data = (BitSet) blk.data.clone(); } 
				catch (Exception e) { e.printStackTrace(); System.exit(1); }
				id = blk.id;
				treeLabel = blk.treeLabel;
				duplicate = blk.duplicate;
				r = blk.r;
				timeStamp = blk.timeStamp;
				crmData = blk.crmData;
			}
			
			Block(BitSet data, int id, int label) {
				assert (data != null) : "Null BitSet data pointer.";
				this.data = data;
				this.id = id;
				this.treeLabel = label;
				this.timeStamp = 0;
				this.duplicate = -1;
				this.crmData = new int[totalNumberOfORAMs];
			}
			
			public Block() {
				data = new BitSet(dataSize*8);
				id = N; // id == N marks a dummy block, so the range of id is from 0 to N, both ends inclusive. Hence the bit length of id is D+1.
				treeLabel = 0;
				timeStamp = 0;
				this.duplicate = -1;
				this.crmData = new int[totalNumberOfORAMs];
				Arrays.fill(this.crmData, -1);
			}
			
			public Block(byte[] bytes) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				duplicate = bb.getInt();
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				id = bb.getInt();
				treeLabel = bb.getInt();
				timeStamp = bb.getInt();
			
				this.crmData = new int[totalNumberOfORAMs];
				for(int i = 0; i < totalNumberOfORAMs; i++){
					crmData[i] = bb.getInt();
				}
				r = new byte[nonceLen];
				bb.get(r);
			}
			
			public Block(byte[] bytes, boolean stash) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				duplicate = bb.getInt();
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				id = bb.getInt();
				treeLabel = bb.getInt();
				timeStamp = bb.getInt();
			
				this.crmData = new int[totalNumberOfORAMs];
				for(int i = 0; i < totalNumberOfORAMs; i++){
					crmData[i] = bb.getInt();
				}
			}
			
			public boolean isDummy() {
				assert (r == null) : "isDummy() was called on encrypted block";
				return id == N;
			}
			
			public void erase() { id = N; treeLabel = 0; }
			
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(extDataSize+nonceLen);
				bb.putInt(duplicate);
				// convert data into a byte array of length 'dataSize'
				byte[] d = new byte[dataSize];
				byte[] temp = data.toByteArray();
				for (int i=0; i<temp.length; i++) {
					d[i] = temp[i];
			    }
				
				bb.put(d);
				
				bb.putInt(id).putInt(treeLabel);
				bb.putInt(timeStamp);
		
				for(int i = 0; i < totalNumberOfORAMs; i++)
					bb.putInt(crmData[i]);
				bb.put((r == null) ? new byte[nonceLen] : r);
					
				return bb.array();
			}
			
			public String toString() {return Arrays.toString(toByteArray());}
			
			private void enc() {
				r = Utils.genPRBits(rnd, nonceLen);
				mask();
			}
			
			private void dec() { 
				mask();
				r = null;
			}
			
			private void mask() {
				byte[] mask = new byte[extDataSize];
				try {
					MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
					int hashLength = 20;
					int i = 0;
					for (; (i+1)*hashLength < extDataSize; i++) {
						sha1.update(clientKey);
						sha1.update(r);
						sha1.update(ByteBuffer.allocate(4).putInt(i));
						System.arraycopy(sha1.digest(), 0, mask, i*hashLength, hashLength);
					}
					sha1.update(clientKey);
					sha1.update(r);
					sha1.update(ByteBuffer.allocate(4).putInt(i));
					System.arraycopy(sha1.digest(), 0, mask, i*hashLength, extDataSize-i*hashLength);

					BitSet dataMask = BitSet.valueOf(Arrays.copyOfRange(mask, 0, dataSize));
					data.xor(dataMask);
					id ^= ByteBuffer.wrap(mask, dataSize, 4).getInt();
					treeLabel ^= ByteBuffer.wrap(mask, dataSize+4, 4).getInt();
					timeStamp ^= ByteBuffer.wrap(mask, dataSize+8, 4).getInt();
					duplicate ^= ByteBuffer.wrap(mask, dataSize+12, 4).getInt();
				//	int ctr = 16;
					//for(int k = 0; k < totalNumberOfORAMs; k++) {
						//crmData[k] = ByteBuffer.wrap(mask, dataSize+ctr, 4).getInt();
						//ctr += 4;
					//}
							
					
				} catch (Exception e) { 
					e.printStackTrace(); 
					System.exit(1); 
				}
			}
		}
		
		class Bucket {
			Block[] blocks = new Block[Z];
			
			Bucket(Block b) {
				assert (b != null) : "No null block pointers allowed.";
				blocks[0] = b;
				for (int i = 1; i < Z; i++)
					blocks[i] = new Block();
			}
			
			Bucket(byte[] array) {
				ByteBuffer bb = ByteBuffer.wrap(array);
				byte[] temp = new byte[extDataSize+nonceLen];
				for (int i = 0; i < Z; i++) {
					bb.get(temp);
					blocks[i] = new Block(temp);
				}
			}
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(Z * (extDataSize+nonceLen));
				for (Block blk : blocks)
					bb.put(blk.toByteArray());
				return bb.array();
			}

			void encryptBlocks() {
				for (Block blk : blocks)
					blk.enc();
			}
		}
		
		class Stash 
		{
			LocalStorage ls = null;
			List<Block> blocks = null;
			int size = 0; // in blocks
			
			public Stash(int size, int recLevel, boolean useLS) 
			{
				this.ls = null;
				this.blocks = null;
				this.size = size;
				
				if(useLS == true)
				{		
					ls = new LocalStorage("/tmp/Local" + recLevel, true);
					ls.connect();
	
					for (int i = 0; i < size; i++) 
					{
						String objectKey = recLevel + "#" + (i);
						DataItem di = new SimpleDataItem(new Block().toByteArray());
						UploadObject upload = new UploadObject(Request.initReqId, objectKey, di);
						ScheduledOperation sop = ls.uploadObject(upload);
						sop.waitUntilReady();
					}
				}
				else
				{
					// use list of blocks (in memory)
					blocks = new ArrayList<Block>();
				}
			}

			public void save(ObjectOutputStream os) throws IOException
			{
				os.writeInt(size);
				os.writeInt(recLevel);
				
				boolean useLS = (ls != null);
				os.writeBoolean(useLS);
								
				if(useLS == true)
				{
					for (int i = 0; i < size; i++) 
					{
						String objectKey = recLevel + "#" + (i);
						DownloadObject download = new DownloadObject(Request.initReqId, objectKey);
						ScheduledOperation sop = ls.downloadObject(download);
						sop.waitUntilReady();
						
						byte[] data = sop.getDataItem().getData();
						
						/*{ // tmp debug
							Block blk = new Block(data, true);
							if(blk.isDummy() == false) 
							{ log.append("[POB (saveStash)] Saving block with id " + blk.id, Log.TRACE); }
						}*/
						
						os.writeInt(data.length);
						os.write(data);
					}
				}
				else
				{
					os.writeInt(blocks.size());
					for(int i=0; i<blocks.size(); i++)
					{
						Block blk = blocks.get(i);
						byte[] data = blk.toByteArray();
						

						/*{ // tmp debug
							if(blk.isDummy() == false) 
							{ log.append("[POB (saveStash)] Saving block with id " + blk.id, Log.TRACE); }
						}*/
						
						os.writeInt(data.length);
						os.write(data);
					}
				}
			}
			
			public Stash(ObjectInputStream is) throws IOException
			{
				this.ls = null;
				this.blocks = null;
				size = is.readInt();
				
				int tmpRecLevel = is.readInt();
				
				boolean useLS = is.readBoolean();
				
				if(useLS == true)
				{
					ls = new LocalStorage("/tmp/Local" + tmpRecLevel, true);
					ls.connect();
					
					for (int i = 0; i < size; i++) 
					{
						int byteSize = is.readInt();
						byte[] data = new byte[byteSize];
						is.readFully(data);
						
						/*{ // tmp debug
							Block blk = new Block(data, true);
							if(blk.isDummy() == false) 
							{ log.append("[POB (loadStash)] Loaded block with id " + blk.id, Log.TRACE); }
						}*/
						
						String objectKey = recLevel + "#" + (i);
						DataItem di = new SimpleDataItem(data);
						UploadObject upload = new UploadObject(Request.initReqId, objectKey, di);
						ScheduledOperation sop = ls.uploadObject(upload);
						sop.waitUntilReady();
					}
				}
				else
				{
					blocks = new ArrayList<Block>();
					
					int s = is.readInt();
					for(int i=0; i<s; i++)
					{
						int byteSize = is.readInt();
						byte[] data = new byte[byteSize];
						is.readFully(data);
						
						Block blk = new Block(data, true);
						
						/*{ // tmp debug
							if(blk.isDummy() == false) 
							{ log.append("[POB (loadStash)] Loaded block with id " + blk.id, Log.TRACE); }
						}*/
						
						blocks.add(blk);
					}
				}
			}
		}

		protected void save(ObjectOutputStream os) throws IOException
		{
			os.writeInt(N);
			os.writeInt(D);

			os.writeLong(treeSize);
			
			stash.save(os);
		}
		
		public Tree(ExternalStorageInterface si, ObjectInputStream is)  throws IOException
		{
			storedTree = si;
			N = is.readInt();
			D = is.readInt();
			
			treeSize = is.readLong();
			
			stash = new Stash(is);
		}
	}
	
	public PathORAMBasicForRec (SecureRandom rand) 
	{
		rnd = rand;
		clientKey = Utils.genPRBits(rnd, keyLen);
	}
	
	/*
	 * Each data has 'unitB' bytes.
	 * 'data' will be intact since the real block has a clone of each element in 'data'.
	 */
	public BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int unitB, BitSet[] data, int recLevel, int rangeSize, int oramID, ArrayList<List<Integer>> permutationList) {
		assert(maxBlocks < (~(1<<63))) : "Too many blocks in a tree.";
		
		this.recLevel = recLevel;
		int nextB = unitB;
		serverTree = new Tree();
		BitSet[] posMap = serverTree.initialize(si, maxBlocks, nextB, data, rangeSize, oramID, permutationList);

		return posMap;
	}

	public enum OpType {Read, Write};

	
	
	
	
	protected Tree.Block[] getStash(long reqID, Tree tr){
		
		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
		Tree.Block[] stash = new Tree.Block[tr.stash.size];
		if(tr.stash.ls != null) // use LS
		{
		for (int i = 0; i < tr.stash.size; i++) 
		{
			String objectKey = recLevel+"#"+(i);
			DownloadObject download = new DownloadObject(reqID, objectKey);
			ScheduledOperation sop = tr.stash.ls.downloadObject(download);
			v.add(sop);
		}
		Pollable.waitForCompletion(v);
		for (int i = 0; i < tr.stash.size; i++) { stash[i] = tr.new Block(v.get(i).getDataItem().getData(), true);}
		}
	else
	{
		for (int i = 0; i < tr.stash.size; i++)
		{
			Tree.Block blk = null;
			if(i < tr.stash.blocks.size()) { blk = tr.stash.blocks.get(i); }
			else { blk = tr.new Block(); }
			stash[i] = blk;
		}
	}
		return stash;
		
	}
	
	
	protected void createAndUploadNewStash(long reqID, Tree tr, ArrayList<Tree.Block> union) {
		
		
		if(tr.stash.ls == null) { tr.stash.blocks.clear(); } // clear stash in memory
		
		int j = 0, k = 0;
		for (; j < union.size() && k < tr.stash.size; j++) 
		{
			if (union.get(j).isDummy() == false) 
			{
				if(union.get(j).id != tr.N)
				//	System.out.println("Uploading new Stash ---" + union[j].id);
				if(tr.stash.ls != null) // use LS
				{
					String objectKey = recLevel + "#" + (k);
					DataItem di = new SimpleDataItem(union.get(j).toByteArray());
					UploadObject upload = new UploadObject(reqID, objectKey, di);
					
					ScheduledOperation sop = tr.stash.ls.uploadObject(upload);
					sop.waitUntilReady();
				}
				else
				{
					tr.stash.blocks.add(tr.new Block(union.get(j)));
				}
				
				k++;
				union.set(j, tr.new Block());
				//union.remove(j);
			}
		}
		if (k == tr.stash.size) 
		{
			for (; j < union.size(); j++)
			{ assert (union.get(j).isDummy()) : "Stash is overflown: " + tr.stash.size + tr.oramID; }	
		}
		
		if(tr.stash.ls != null) // use LS
		{
			while (k < tr.stash.size) 
			{
				String objectKey = recLevel + "#" + (k);
				DataItem di = new SimpleDataItem(tr.new Block().toByteArray());
				UploadObject upload = new UploadObject(reqID, objectKey, di);
				ScheduledOperation sop = tr.stash.ls.uploadObject(upload);
				sop.waitUntilReady();
		
				k++;
			}
		
		}
		
		
	}

	protected Tree.Block[] access(long reqId, BitSet[] posMap, OpType op, int a, BitSet data)
	{
		Tree tr = serverTree; 
		Tree.Block[] res = new Tree.Block[tr.rangeSize];
		
		tr.uploadBytes = 0;
		tr.downloadBytes = 0;
		tr.seeks = 0;
		
		int leafLabel = Utils.readPositionMap(posMap, tr, a);
		int newLabel = rnd.nextInt(tr.N);
		Utils.writePositionMap(posMap, tr, a, newLabel); //(u % s.C)*s.D, newlabel, s.D);				
		Tree.Block[] result =  tr.batchReadRange(reqId, (leafLabel), ((tr.rangeSize)*a), tr.rangeSize);

	
			
			for(int j = 0; j < tr.rangeSize; j++) {
				res[j] = result[j];
		
				
				if(res[j] == null)
				{
					System.out.println("[POB" + this.recLevel + "Couldn't find block with id " + (a*tr.rangeSize+j) + " in the path down to label " + (leafLabel+j)+ " or in the stash.");
					System.exit(0);
				}
				res[j].treeLabel = newLabel+j;
				res[j].duplicate = -1;
				if(op == OpType.Write)
					res[j].data = data;
		}
		
			tr.seekLog.info("ORAM_Access: " + tr.oramID + "Seeks: " + tr.seeks);
			tr.downLog.info("ORAM_Access: " + tr.oramID + "Download: " + tr.downloadBytes+ "bytes");
			tr.upLog.info("ORAM_Access: " + tr.oramID + "Upload: " + tr.uploadBytes+ "bytes");
	
			
		return res;
	//	return rearrangeBlocksAndReturn(reqId, res, tr.rangeSize);
	}


	
	public PathORAMBasicForRec.Tree.Block[] rearrangeBlocksAndReturn(long reqID, Tree.Block[] blocks, int batchSize)
	{
	
		
	
		
		Tree tr = serverTree;
		

		tr.uploadBytes = 0;
		tr.downloadBytes = 0;
		tr.seeks = 0;
		
		Tree.Block[] stash = getStash(reqID, tr);	
	
		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
	
		if(batchSize >= tr.a)
			batchSize = batchSize/tr.a;
		else
			batchSize = 1;
		
		int nextLeafLabel = (int) tr.evictCounter; 
		
		ArrayList<Tree.Block> union = tr.batchReadPaths(reqID, nextLeafLabel, batchSize);
		HashMap<Integer, Tree.Block> duplicateList = new HashMap<Integer, Tree.Block>();

	
		
		for(Tree.Block blk: stash) {
			
	//		System.out.println(blk.id + " ---" + (Utils.readPositionMap(tr.posMap, tr, rangeID) + rangeOffset));
			if(blk.id != tr.N) {  //(blk.treeLabel == (Utils.readPositionMap(posMap, tr, rangeID) + rangeOffset))) {	// this is really hacky			
	
			if(duplicateList.containsKey(blk.id)) {
				
					if(duplicateList.get(blk.id).timeStamp > blk.timeStamp)
						blk = tr.new Block();
					else
						duplicateList.replace(blk.id, blk);
					}
				else
					duplicateList.put(blk.id, blk);
				}
			
		}
		
	
		for(Tree.Block blk: blocks) {
			
	//		System.out.println(blk.id + " ---" + (Utils.readPositionMap(tr.posMap, tr, rangeID) + rangeOffset));
			if(blk.id != tr.N) {  //(blk.treeLabel == (Utils.readPositionMap(posMap, tr, rangeID) + rangeOffset))) {	// this is really hacky			
	
			if(duplicateList.containsKey(blk.id)) {
				
					if(duplicateList.get(blk.id).timeStamp > blk.timeStamp)
						blk = tr.new Block();
					else
						duplicateList.replace(blk.id, blk);
					}
				else
					duplicateList.put(blk.id, blk);
				}
		}
		
		
		for(int i = 0; i < union.size(); i++) {
			Tree.Block blk = union.get(i);
			if(blk.r != null) { blk.dec();}
		
	//		System.out.println(blk.id + " ---" + (Utils.readPositionMap(tr.posMap, tr, rangeID) + rangeOffset));
			if(blk.id != tr.N && (blk.duplicate != 1)) {  //(blk.treeLabel == (Utils.readPositionMap(posMap, tr, rangeID) + rangeOffset))) {	// this is really hacky			
			
				if(duplicateList.containsKey(blk.id)) {
				
					if(duplicateList.get(blk.id).timeStamp > blk.timeStamp)
						blk = tr.new Block();
					else
						duplicateList.replace(blk.id, blk);
					}
				else
					duplicateList.put(blk.id, blk);
				}
		}

		union.clear();		
		for(int key: duplicateList.keySet()) { union.add(tr.new Block(duplicateList.get(key))); }
			
		Collections.shuffle(union, rnd);
		


		ArrayList<Tree.Block> blockList = new ArrayList<Tree.Block>();
		int bucketLabel[] = new int[tr.D+1];
		int numberOfBuckets[] = new int[tr.D+1];
		
		/*
		for(int i = 0; i < union.size();i++) {
			if(union.get(i).id != tr.N && recLevel == 0)
				System.out.println("stash ---" + union.get(i).id);
		}
		*/
			
					
			for (int i = tr.D; i >= 0; i--) 
			{
	
				HashMap<Integer, Tree.Bucket> bucketList = new HashMap<Integer, Tree.Bucket>();

				
				for(int l = 0; l < Math.min(Math.pow(2, i),batchSize); l++ ) {
					int depth = i;
					int key;
					int leafLabel = (nextLeafLabel+l)%tr.N;
				
					if(i > 0)
						key = (int) (leafLabel%Math.pow(2,i));
					else
						key = 0;
				
				String objectKey = recLevel + "ORAM #" + tr.oramID + "depth" + i + "#"+ key;
		//		System.out.println("objectKey" + objectKey);
				
				Tree.Bucket bucket;
			
				
				if(bucketList.containsKey(key))
						bucket = bucketList.get(key);	
				else
					    bucket = tr.new Bucket(tr.new Block());
				 
			
				
					 for (int j = 0, k = 0; j < union.size() && k < Z; j++) 
				{
					if (!union.get(j).isDummy())						
					{ 
					
					//	System.out.println("****" + union[j].id);
						if(leafLabel%Math.pow(2, i) == union.get(j).treeLabel%Math.pow(2, i)) 
						{
							
							bucket.blocks[k] = tr.new Block(union.get(j));
							bucket.blocks[k++].timeStamp = (int) tr.evictCounter;
							//if(recLevel == 0)
							//	System.out.println("[R" +recLevel+"] Writing block " + bucket.blocks[k-1].id + "with timestamp " + bucket.blocks[k-1].timeStamp+  "which is mapped to " + bucket.blocks[k-1].treeLabel + "to " + objectKey);
							union.set(j,tr.new Block());
				//			duplicateList.remove(union.get(j).id);
							//union.set(j,tr.new Block());
							//	union.remove(j);
								
						}
					}
				}
			
			

			if(bucketList.containsKey(key)) 
				 bucketList.replace(key, bucket);
			else
				 bucketList.put(key, bucket);
			
			}
			
				if(i > 0)
					bucketLabel[i] = (int) (nextLeafLabel%Math.pow(2,i));
				else
					bucketLabel[i] = 0;
			
				
		
				for(int j = 0; j < bucketList.keySet().size(); j++) {
					//	System.out.println(bucketLabel+j)%Math.pow(2, i));	
						Tree.Bucket bucket = bucketList.get((bucketLabel[i]+j)%((int) Math.pow(2, i)));
											
						for(Tree.Block blk: bucket.blocks) {
							//if(blk.id != tr.N)
								//System.out.println("Writing " + blk.id);
							blk.enc();
							blockList.add(blk);
						//bucket.encryptBlocks();
						
						}	
				}
				
				numberOfBuckets[i] = bucketList.keySet().size();
		}
	
			tr.uploadChunk(reqID, bucketLabel, numberOfBuckets, blockList);
			
			tr.evictCounter = (tr.evictCounter+batchSize)%tr.N;

			
			createAndUploadNewStash(reqID, tr, union);
		
			tr.seekLog.info("ORAM_Eviction: " + tr.oramID + "Seeks: " + tr.seeks);
			tr.downLog.info("ORAM_Eviction: " + tr.oramID + "Download: " + tr.downloadBytes+ "bytes");
			tr.upLog.info("ORAM_Eviction: " + tr.oramID + "Upload: " + tr.uploadBytes+ "bytes");

			
	//System.out.println("Eviction Done");
		
		return blocks;
		
	}
	
	private Set<Integer> getPathString(int leaf) 
	{
		Set<Integer> nodesList = new TreeSet<Integer>();
		int temp = leaf;
		while(temp >= 0)
		{
			nodesList.add(temp);
			temp = ((temp+1)>>1)-1;
		}
		return nodesList;
	}

	public Tree.Block[] read(long reqID, BitSet[] pm, int i) { return access(reqID, pm, OpType.Read, i, null); }
	public Tree.Block[] write(long reqID, BitSet[] pm, int i, BitSet d) { return access(reqID, pm, OpType.Write, i, d); }
	public void batchEvict(long reqID, BitSet[] pm, Tree.Block[] blocks, int batchSize) { 
		
		for(Tree.Block blk: blocks) {
			Tree tr = serverTree;
			blk.timeStamp = (int) (tr.evictCounter);
			int rangeLabel = Math.floorDiv(blk.id, tr.rangeSize);					
			int pathLabel =   Utils.readPositionMap(pm, tr, rangeLabel);
			int rangeOffset = blk.id%tr.rangeSize;
			blk.treeLabel = pathLabel + rangeOffset;
		
		
		}
		rearrangeBlocksAndReturn(reqID, blocks, batchSize);
	}
	
	static class Utils {
		/*
		 * Given 'n' that has 'w' bits, output 'res' whose most significant 'i' bits are identical to 'n' but the rest are 0.
		 */
		static int iBitsPrefix(int n, int w, int i) {
			return (~((1<<(w-i)) - 1)) & n;
		}
		
		/*
		 * keys, from 1 to D, are sorted in ascending order
		 */
		static void refineKeys(int[] keys, int d, int z) {
			int j = 0, k = 1;
			for (int i = 1; i <= d && j < keys.length; i++) {
				while (j < keys.length && keys[j] == i) {
					keys[j] = (i-1)*z + k;
					j++; k++;
				}
				if (k <= z) 
					k = 1;
				else
					k -= z;
			}
		}
		
		
//		private static <T> void swap(T[] arr, int i, int j) {
//			T temp = arr[i];
//			arr[i] = arr[j];
//			arr[j] = temp;
//		}
		
		static void writePositionMap(BitSet[] map, Tree st, int index, int val) {
			int base = (index % C) * st.D;
			writeBitSet(map[index/C], base, val, st.D);
		}
		
		static BitSet writeBitSet(BitSet map, int base, int val, int d) {
			for (int i = 0; i < d; i++) {
				if (((val>>i) & 1) == 1)
					map.set(base + i);
				else
					map.clear(base + i);
			}
			
			return map;
		}
		
		static BitSet writeBitSet(BitSet map, int base, BitSet val, int d) {
			for (int i = 0; i < d; i++) {
				if (val.get(i))
					map.set(base + i);
				else
					map.clear(base + i);
			}
			
			return map;
		}
		
		static int readPositionMap(BitSet[] map, Tree st, int index) {
			int base = fastMod(index, C) * st.D;
			int mapIdx = fastDivide(index, C);
			if(mapIdx >= map.length) { Errors.error("Coding FAIL!"); }
			return readBitSet(map[mapIdx], base, st.D);
		}
		
		static int readBitSet(BitSet map, int base, int d) {
			int ret = 0;
			for (int i = 0; i < d; i++) {
				if (map.get(base + i) == true)
					ret ^= (1<<i);
			}
			return ret;
		}
		
		/* 
		 * n has to be a power of 2.
		 * Return the number of bits to denote n, including the leading 1.
		 */
		static int bitLength(int n) {
			if (n == 0) 
				return 1;
			
			int res = 0;
			do {
				n = n >> 1;
				res++;
			} while (n > 0);
			return res;
		}
		
		static int fastMod(int a, int b) {
			// b is a power of 2
			int shifts = (int) (Math.log(b)/Math.log(2));
			return  a & (1<<shifts) - 1;
		}
		
		static int fastDivide(int a, int b) {
			// b is a power of 2
			int shifts = (int) (Math.log(b)/Math.log(2));
			return  a >> shifts;
		}
		
		static byte[] genPRBits(SecureRandom rnd, int len) {
			byte[] b = new byte[len];
			rnd.nextBytes(b);
			return b;
		}
	}

	public void recursiveSave(ObjectOutputStream os) throws IOException
	{
		os.writeInt(dataSize);
		os.writeInt(extDataSize);
		//os.writeInt(keyLen); os.writeInt(nonceLen);
		os.writeInt(Z); os.writeInt(C);
		os.writeInt(stashSize);
		
		serverTree.save(os);
		
		os.writeInt(recLevel);
		
		os.write(clientKey);		
	}
	
	protected void initializeLoad(ExternalStorageInterface si, ObjectInputStream is) throws IOException
	{
		dataSize = is.readInt();
		extDataSize = is.readInt();
		
		//keyLen = is.readInt(); nonceLen = is.readInt();
		Z = is.readInt(); C = is.readInt();
		stashSize = is.readInt();
		
		serverTree = new Tree(si, is);
		
		recLevel = is.readInt();
		clientKey = new byte[keyLen];
		is.readFully(clientKey);
	}

	public void recursiveLoad(ExternalStorageInterface esi, ObjectInputStream is, int i) throws IOException 
	{
		initializeLoad(esi, is);		
	}

	public int getRecursionLevels() { return 0; }


	
}
