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

public class BatchEvictPathORAMForRec {

	
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
		/*
		 * Given input "data" (to be outsourced), initialize the server side storage and return the client side position map. 
		 * No recursion on the tree is considered. 
		 */
		
		
		

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
			this.a = 1;
					
			evictCounter = 0;
        	this.posMap = buildTree(maxBlocks, data, oramID, permutationList);
			stash = new Stash(stashSize, recLevel, stashUseLS);
 			
			return this.posMap;
		}

		private BitSet[] buildTree(int maxBlocks,  BitSet[] dataArray, int oramID, ArrayList<List<Integer>> permutationList) 
		{
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
							if(k != oramID){
								int rSize = (int) Math.pow(2, k);
								int rLabel = Math.floorDiv(id, rSize);
								int rOffset = (id)%rSize;
								int rID = permutationList.get(k).get(rLabel)*rSize + rOffset; 
								blk.crmData[k] = rID;
						
						
							}
							else 
								blk.crmData[k] = i-N+1;
						}
						temp = new Bucket(blk);
								
						
					//	{ System.out.println("[PathORAMBasic (BuildTree)] (R" + oramID +") putting block " + id + " to label " + (i-N+1) + " (objectKey: " + recLevel + "depth" + getDepth(i) + "#" + (i-N+1)); }
					}
					else
						temp = new Bucket(new Block());
				}
				
				temp.encryptBlocks();
				
	
		
				DataItem di = new SimpleDataItem(temp.toByteArray());
				int fileLabel = Math.floorDiv(i,bucketLimitPerFile);
				String objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
				int fileOffset = i%(bucketLimitPerFile);
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
			
			Pollable.waitForCompletion(v);
			
			return posMap;
		}

		

		/* 
		 * Download a sequential chunk of buckets from each level 
		 * startingBucket - indicate which bucket to start fetching from in each level
		 * numOfBuckets - number of buckets to fetch from each level
		 */
		
		private ArrayList<Tree.Block> downloadChunk(long reqId, int startingBucket[], int numOfBuckets[]) {

			
			DownloadObject download; 
			String objectKey;
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
			int[] bucketsUptoEnd = new int[D+1]; 
			int[] bucketsFromStart = new int[D+1]; 
			ArrayList<Integer> pollableList = new ArrayList<Integer>();
		
			for(int i=D; i >= 0; i--){
				bucketsUptoEnd[i] = 0;
				bucketsFromStart[i] = 0;
				int startingOffsetAtLevel;
		
				/*  Identify wrap around for level based on numOfBuckets */
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

					/* Retrieve buckets up to end of the level starting from startBucket */
					while(temp < bucketsUptoEnd[i]) {
						if(temp + fileOffset >= bucketLimitPerFile) {
							objectKey = Integer.toString(oramID)+"#" + recLevel + Integer.toString(fileLabel);
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
					
					
					/* Retrieve buckets after wrap around from start of level */
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
					download = new DownloadObject(reqId, objectKey);
					download.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
					download.setObjectSize(Z*(extDataSize + nonceLen)*cntr);
					sop = storedTree.downloadObject(download);
					pollableList.add(cntr);
					v.add(sop);	
					}
				}
				
			Pollable.waitForCompletion(v);

			
			/* Reconcile all fetched items and return block list */
			ArrayList<Tree.Block> blockList = new ArrayList<Tree.Block>();
			for(int k = 0; k < pollableList.size(); k++) {
				byte[] b = v.get(k).getDataItem().getData();
				byte[] bucket = new byte[Z*(extDataSize+nonceLen)];
	
		
				
				for(int j = 0; j < pollableList.get(k); j++) {
					for(int i = 0; i < Z*(extDataSize+nonceLen); i++) { 
						bucket[i] = b[j*(Z*(extDataSize+nonceLen))+i];
							
					}
					Tree.Bucket bkt = new Tree.Bucket(bucket);
					for(Tree.Block blk : bkt.blocks)
						blockList.add(blk);
				}	
			}
		
		
			return  blockList;
			
			}
	
		
		/* 
		 * Upload a sequential chunk of buckets to each level 
		 * startingBucket - indicate which bucket to start uploading to in each level
		 * numOfBuckets - number of buckets to upload to each level
		 * blockList - blocks to upload
		 */
		
	
	
		
		private void uploadChunk(long reqId, int startingBucket[], int numOfBuckets[], ArrayList<Tree.Block> blockList) {
			
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
			String objectKey;
			UploadObject upload;
			
			int startingOffsetAtLevel;
			int nextBlock = 0;
			
			for(int level = D; level >= 0; level--) {
				
				/*  Identify wrap around for level based on numOfBuckets */
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
			
		
				
				int temp = 0;
				int fileLabel = Math.floorDiv(startingBucket[level]+startingOffsetAtLevel,bucketLimitPerFile);
				int fileOffset = (startingBucket[level]+startingOffsetAtLevel)%(bucketLimitPerFile);
				int cntr = 0;
				
				/* upload buckets till end of level from starting bucket */
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
				
				
				/* upload buckets after wrap around from start of level */
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
			}
			Pollable.waitForCompletion(v);
		}
		
		/*  
		 * Read and return blocks along adjacent (on storage) path(s)
		 * leaf - starting leaf identifier
		 * batchSize - number of adjacent paths to read
		 */
		
		
		private ArrayList<Tree.Block> batchReadPaths(long reqId, int leaf,int batchSize) 
		{
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

		
		/*  
		 * Read and return a range of blocks
		 * leafLabel - starting leaf identifier
		 * a - range start address
		 * rangeSize - number of adjacent paths to read
		 */
		
		
		public Block[] batchReadRange(long reqId, int leafLabel, int a, int rangeSize) {

			Tree tr = serverTree;
			Block[] stash = getStash(reqId,tr);
			Block[] result = new Tree.Block[rangeSize];
			
			Arrays.fill(result, null);
			for(Block blk : stash) {
				if (blk.r != null) // when r == null, the block comes from the stash, already decrypted.
				{ blk.dec(); }
				
			}
		
			/* Read corresponding paths from the tree */
			ArrayList<Tree.Block> blockList = batchReadPaths(reqId, leafLabel, rangeSize);
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
						}
					}	
				}
					blockList.get(i).enc();
			}
			
			
			/* Check for duplicates and keep the most recent version */
			for(int j = 0; j < rangeSize; j++) {
				for(Tree.Block blk: stash) {
					if(blk.id != tr.N) {	
						if(blk.id == (a+j) && (blk.timeStamp > timeStamps[j])) {
							result[j] = tr.new Block(blk);
							timeStamps[j] = blk.timeStamp;

					}
				}
				}
				result[j].timeStamp = (int) evictCounter+1;
			}
			
			/* Write back blocks to path to indicating stale/duplicate blocks*/ 
			 // TODO: This can be done more efficiently 
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
			uploadChunk(reqId, bucketLabel, numberOfBuckets, blockList);
			return result;
		}

		


		public class Block {
			int duplicate = -1;
			public BitSet data;
			public int id; // range: 0...N-1;
			public int treeLabel;
			public int timeStamp;
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
				crmData = new int[totalNumberOfORAMs];
				for (int i = 0; i < crmData.length; i++)
					crmData[i] = blk.crmData[i];
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
					
					int ctr = 16;
					for(int k = 0; k < totalNumberOfORAMs; k++) {
						crmData[k] ^= ByteBuffer.wrap(mask, dataSize+ctr, 4).getInt();
						ctr += 4;
					}
							
					
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
	
	public BatchEvictPathORAMForRec (SecureRandom rand) 
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
		return res;
	}


	
	public BatchEvictPathORAMForRec.Tree.Block[] rearrangeBlocksAndReturn(long reqID, Tree.Block[] blocks, int batchSize)
	{
		Tree tr = serverTree;
		Tree.Block[] stash = getStash(reqID, tr);	
		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
		if(batchSize >= tr.a)
			batchSize = batchSize/tr.a;
		else
			batchSize = 1;
		int nextLeafLabel = (int) tr.evictCounter; 
		ArrayList<Tree.Block> union = tr.batchReadPaths(reqID, nextLeafLabel, batchSize);
		HashMap<Integer, Tree.Block> duplicateList = new HashMap<Integer, Tree.Block>();

		/* add blocks retrieved from path to duplicate list */
		for(Tree.Block blk: blocks) {
			if(blk.id != tr.N) {  		
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
		
	
		/* add blocks from stash to duplicate list */
		for(Tree.Block blk: stash) {
			if(blk.id != tr.N) { 		
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
		

		/* remove duplicates */
		for(int i = 0; i < union.size(); i++) {
			Tree.Block blk = union.get(i);
			if(blk.r != null) { blk.dec();}
			if(blk.id != tr.N && (blk.duplicate != 1)) {  			
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
		
		/* debug only
		for(int i = 0; i < union.size();i++) {
			if(union.get(i).id != tr.N && recLevel == 0)
				System.out.println("stash ---" + union.get(i).id);
		}
		*/
			
			/* Evict blocks from the union to the tree level-by-level */		
			for (int i = tr.D; i >= 0; i--) 
			{
				HashMap<Integer, Tree.Bucket> bucketList = new HashMap<Integer, Tree.Bucket>();
				for(int l = 0; l < Math.min(Math.pow(2, i),batchSize); l++ ) {
					int key;
					int leafLabel = (nextLeafLabel+l)%tr.N;
				
					if(i > 0)
						key = (int) (leafLabel%Math.pow(2,i));
					else
						key = 0;
				
				//String objectKey = recLevel + "ORAM #" + tr.oramID + "depth" + i + "#"+ key;
				Tree.Bucket bucket;
				if(bucketList.containsKey(key))
						bucket = bucketList.get(key);	
				else
					    bucket = tr.new Bucket(tr.new Block());
				
					 for (int j = 0, k = 0; j < union.size() && k < Z; j++) 
					 {
						 if (!union.get(j).isDummy())						
						 { 
					
							 if(leafLabel%Math.pow(2, i) == union.get(j).treeLabel%Math.pow(2, i)) 
							 {
								 bucket.blocks[k++] = tr.new Block(union.get(j));
								 union.set(j,tr.new Block());
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
					Tree.Bucket bucket = bucketList.get((bucketLabel[i]+j)%((int) Math.pow(2, i)));
					for(Tree.Block blk: bucket.blocks) {
						blk.enc();
						blockList.add(blk);
					}	
			}
			numberOfBuckets[i] = bucketList.keySet().size();
		}
	
		/* write back update paths to tree and upload new stash */
		tr.uploadChunk(reqID, bucketLabel, numberOfBuckets, blockList);
		tr.evictCounter = (tr.evictCounter+batchSize)%tr.N;
		createAndUploadNewStash(reqID, tr, union);
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
