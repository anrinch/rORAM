package pathORAM;

import java.lang.instrument.Instrumentation;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sound.midi.Instrument;

import pollable.Pollable;
import utils.*;
import backend.*;
import data.*;
import services.*;
import services.Request.RequestType;
import services.Service.ObjectType;
import Interfaces.*;


public class PathORAMBasic
{
	protected Log log = Log.getInstance();
	
	protected SecureRandom rnd;
	int dataSize; // measured in byte.
	int extDataSize; // measured in byte.
	static final int keyLen = 10; // length of key (to encrypt each data piece) in bytes
	static final int nonceLen = 10; // length of nonce (to encrypt each data piece) in bytes

	public static int bucketLimitPerFile = (int) Math.pow(2, 16);
	public static int Z = 4;
	public static int stashSize = 89; // set according to table 3 of PathORAM paper (for 80-bit security)
	
	//public static int C = 1024;
	public static int C = 4; // memory reduction factor (ensure it is a power of 2)
	
	public static final boolean stashUseLS = false;
	//public static final boolean stashUseLS = true;
	
	Tree serverTree;
	int recLevel;
	
	byte[] clientKey;
	
	public class Tree {
		public int N; // the number of logic blocks in the tree
		public int D; // depth of the tree
//		int dataLen; // data length in bits
		
		ExternalStorageInterface storedTree;
		long treeSize;
		public Stash stash;
		int oramID;
		
		Tree() { }

		/*
		 * Given input "data" (to be outsourced), initialize the server side storage and return the client side position map. 
		 * No recursion on the tree is considered. 
		 */
		private BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int dSize, BitSet[] data, int oramID) 
		{
			storedTree = si;
			dataSize = dSize;
			extDataSize = dataSize + 4 + 4;
			
			// generate a random permutation for init, so we don't get a silly stash overflow
			List<Integer> permutation = new ArrayList<Integer>();
            for (int i = 0; i < maxBlocks; i++) { permutation.add(i); }
            Collections.shuffle(permutation);
            
			// TODO
            this.oramID = oramID;
            buildTree(maxBlocks, permutation, data);
			stash = new Stash(stashSize, recLevel, stashUseLS);
			
			

            // setup the position map according to the permuted blocks
 			BitSet[] posMap = new BitSet[(N + C-1) / C];	// clientPosMap[i] is the leaf label of the i-th block.
 			for (int i = 0; i < posMap.length; i++)	{ posMap[i] = new BitSet(C*D); }

 			for (int i = 0; i < N; i++) 
 			{
 				int p = i;
 				if(i < permutation.size()) { p = permutation.get(i); }
 				Utils.writePositionMap(posMap, this, i, p); 
 				
 				//{ log.append("[POB (initialize)] Block " + i + " -> leaf " + p, Log.TRACE); }
 			}

			return posMap;
		}

		private void buildTree(int maxBlocks, List<Integer> permutation, BitSet[] dataArray) 
		{
//			storedTree = new LocalStorage();
//			storedTree.initialize(null, "/tmp/Cloud" + recLevel);
			
			SessionState ss = SessionState.getInstance();
			Map<String, Request> fastInitMap = ss.fastInitMap;
			if(ss.fastInit == false) {  fastInitMap = null; }
			
			
			//String objectKey = Integer.toString(this.oramID)  +recLevel;

			
			// set N to be the smallest power of 2 that is bigger than 'data.length'. 
			N = (int) Math.pow(2, Math.ceil(Math.log(maxBlocks)/Math.log(2)));
			D = Utils.bitLength(N)-1;
			
			
			final int removeIntervalSize = 512; final double sizeFactorForSlowdown = 0.75;
			final int logIntervalSize = 512;
			Vector<Pollable> v = new Vector<Pollable>();

			// initialize the tree
			treeSize = 2*N-1;
			for (int i = 0; i < treeSize; i++) 
			{
				Bucket temp;
				if (i < treeSize/2) { temp = new Bucket(new Block()); }
				else {
					if (i-N+1 < maxBlocks)
					{
						int id = permutation.indexOf(i-N+1); // make sure we are consistent with the permutation
						int label = i-N+1;
						
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
						
						temp = new Bucket(new Block(data, id, label));
						
						//{ System.out.println("[PathORAMBasic (BuildTree)] (R" + recLevel +") putting block " + id + " to label " + label + " (objectKey: " + recLevel + "#" + (i) + ")."); }
					}
					else
						temp = new Bucket(new Block());
				}
				
				temp.encryptBlocks();
			
				DataItem di = new SimpleDataItem(temp.toByteArray());
				int fileLabel = Math.floorDiv(i,bucketLimitPerFile);
				String objectKey = Integer.toString(oramID)+ recLevel + Integer.toString(fileLabel);
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
					//Log.getInstance().append("[PathORAMBasic (BuildTree)] Uploaded " + (i - v.size()) + " nodes so far.", Log.TRACE);
				}
			}
			
			// This waitForCompletion ensures can be used asynchronously!
			Pollable.waitForCompletion(v);
		}

		
		protected Block[] readBuckets(long reqId, int leafLabel) {
			
			Bucket[] buckets = getBucketsFromPath(reqId, leafLabel);
			Block[] res = new Block[Z*buckets.length];
			int i = 0;
			for (Bucket bkt : buckets) 
			{
				for (Block blk : bkt.blocks)
				{
					/*{ // debug only
						Block blk2 = new Block(blk);
						blk2.dec();
						if(blk2.isDummy() == false) { log.append("[POB (readBuckets)] Found block with id " + blk2.id, Log.TRACE); }
					}*/
					res[i++] = new Block(blk);
				}
			}
			
			return res;
		}

		private Bucket[] getBucketsFromPath(long reqId, int leaf) 
		{
		//	System.out.println("Fetch start");
			Bucket[] ret = new Bucket[D+1];
			
			Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();

		//	String objectKey = Integer.toString(oramID) + recLevel;
			int temp = leaf; //((leaf+1)>>1)-1;
			for (int i = 0; i < ret.length; i++) 
			{
			
				int fileLabel = Math.floorDiv(temp,bucketLimitPerFile);
				String objectKey = Integer.toString(oramID)+ recLevel + Integer.toString(fileLabel);
				int fileOffset = temp%(bucketLimitPerFile);
		
				DownloadObject download = new DownloadObject(reqId, objectKey);
				download.setObjectOffset((fileOffset)*Z*(extDataSize + nonceLen));
				download.setObjectSize(Z*(extDataSize + nonceLen));
				ScheduledOperation sop = storedTree.downloadObject(download);
				v.add(sop);
				
				// debug only
				//{ log.append("[POB (getBucketsFromPath)] reading down to leaf " + leaf + " (" + (leaf - (N-1))  + ") objectKey: " + objectKey, Log.ERROR); }
				
				if (temp > 0) { temp = ((temp+1)>>1)-1; }
			}
			
			Pollable.waitForCompletion(v);
			for (int i = 0; i < ret.length; i++) { ret[i] = new Bucket(v.get(i).getDataItem().getData()); }
			
			return ret;
		}


		public class Block {
			public BitSet data;
			int id; // range: 0...N-1;
			int treeLabel;
			
			private byte[] r; 
			
			public Block(Block blk) {
				assert (blk.data != null) : "no BitSet data pointers is allowed to be null.";
				try { data = (BitSet) blk.data.clone(); } 
				catch (Exception e) { e.printStackTrace(); System.exit(1); }
				id = blk.id;
				treeLabel = blk.treeLabel;
				r = blk.r;
			}
			
			Block(BitSet data, int id, int label) {
				assert (data != null) : "Null BitSet data pointer.";
				this.data = data;
				this.id = id;
				this.treeLabel = label;
			}
			
			public Block() {
				data = new BitSet(dataSize*8);
				id = N; // id == N marks a dummy block, so the range of id is from 0 to N, both ends inclusive. Hence the bit length of id is D+1.
				treeLabel = 0;
			}
			
			public Block(byte[] bytes) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				bb = bb.get(bs);	// debug only
				//{ log.append("[POB (getBucketsFromPath)] reading down to leaf " + leaf + " (" + (leaf - (N-1))  + ") objectKey: " + objectKey, Log.ERROR); }
				
			//		if (temp > 0) { temp = ((temp+1)>>1)-1; }
				data = BitSet.valueOf(bs);
				id = bb.getInt();
				treeLabel = bb.getInt();
				r = new byte[nonceLen];
				bb.get(r);
			}
			
			public Block(byte[] bytes, boolean stash) {
				byte[] bs = new byte[dataSize];
				ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
				bb = bb.get(bs);
				data = BitSet.valueOf(bs);
				id = bb.getInt();
				treeLabel = bb.getInt();
			}
			
			public boolean isDummy() {
				assert (r == null) : "isDummy() was called on encrypted block";
				return id == N;
			}
			
			public void erase() { id = N; treeLabel = 0; }
			
			
			public byte[] toByteArray() {
				ByteBuffer bb = ByteBuffer.allocate(extDataSize+nonceLen);

				// convert data into a byte array of length 'dataSize'
				byte[] d = new byte[dataSize];
				byte[] temp = data.toByteArray();
				for (int i=0; i<temp.length; i++) {
					d[i] = temp[i];
			    }
				
				bb.put(d);
				
				bb.putInt(id).putInt(treeLabel);
				
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
	
	public PathORAMBasic (SecureRandom rand) 
	{
		rnd = rand;
		clientKey = Utils.genPRBits(rnd, keyLen);
	}
	
	/*
	 * Each data has 'unitB' bytes.
	 * 'data' will be intact since the real block has a clone of each element in 'data'.
	 */
	public BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int unitB, BitSet[] data, int recLevel,int oramID) {
		assert(maxBlocks < (~(1<<63))) : "Too many blocks in a tree.";
		
		this.recLevel = recLevel;
		int nextB = unitB;
		serverTree = new Tree();
		BitSet[] posMap = serverTree.initialize(si, maxBlocks, nextB, data, oramID);

		return posMap;
	}

	public enum OpType {Read, Write};
	protected Tree.Block access(long reqId, BitSet[] posMap, OpType op, int a, BitSet data)
	{
		Tree tr = serverTree;

		int leafLabel = tr.N-1 + Utils.readPositionMap(posMap, tr, a);
		Tree.Block[] blocks = tr.readBuckets(reqId, leafLabel);
		
		int newLabel = rnd.nextInt(tr.N);
		Utils.writePositionMap(posMap, tr, a, newLabel); //(u % s.C)*s.D, newlabel, s.D);
		
		// debug only
		//{ log.append("[POB (access)] (R" + recLevel + ") Block with id " + a + " should be in the path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.N-1)) + ") or in the stash (new label: " + newLabel + ").", Log.TRACE); }
		
		return rearrangeBlocksAndReturn(reqId, op, a, data, leafLabel, blocks, newLabel);
	}

	protected Tree.Block rearrangeBlocksAndReturn(long reqID, OpType op, int a, BitSet data, int leafLabel, Tree.Block[] blocks, int newlabel)
	{
		Tree tr = serverTree;
		Tree.Block res = null;
		Tree.Block[] union = new Tree.Block[tr.stash.size+blocks.length];
		
		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
//		String objectKey = Integer.toString(tr.oramID) + recLevel;
		
		
		
			for (int i = 0; i < tr.stash.size; i++)
			{
				Tree.Block blk = null;
				if(i < tr.stash.blocks.size()) { blk = tr.stash.blocks.get(i); }
				else { blk = tr.new Block(); }
				union[i] = blk;
			}
		
		for (int i = 0; i < blocks.length; i++) { union[i+tr.stash.size] = tr.new Block(blocks[i]); }
		
		for (Tree.Block blk : union)
		{
			if (blk.r != null) // when r == null, the block comes from the stash, already decrypted.
			{ blk.dec(); }
		}
		for (int i = 0; i < union.length; i++)
		{
			Tree.Block blk = union[i];
			if (blk.id == a) {
				res = tr.new Block(blk);
				blk.treeLabel = newlabel;
				if (op == OpType.Write) {
					blk.data = data;
				}
			}
		}
		
		/*
		{ // debug only
			for (int i = 0; i < tr.stash.size; i++) 
			{ 
				Tree.Block blk = union[i]; 
				if(blk.isDummy() == false) 
				{ log.append("[POB (rearrangeBlocksAndReturn)] Stash contains block with id " + blk.id, Log.TRACE); }
			}
			for (int i = 0; i < blocks.length; i++) 
			{ 
				Tree.Block blk = union[i+tr.stash.size]; 
				if(blk.isDummy() == false) { log.append("[POB (rearrangeBlocksAndReturn)] Path down to label " + (leafLabel - (tr.N-1)) + " contain block with id " + blk.id, Log.TRACE); }
			}
		}*/
		if(res == null)
		{
			Errors.error("[POB (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (tr.N-1))+ " or in the stash.");
		}
		
		List<Tree.Block> unionList = Arrays.asList(union);
		Collections.shuffle(unionList, rnd);
		union = unionList.toArray(new Tree.Block[0]);
		
		/** // this code is equivalent to (and easier to follow than) the piece right below
		 
		v.clear();
		Set<Integer> pathSet = getPathString(leafLabel); System.out.println(pathSet);
		Map<Integer, Tree.Bucket> pathMap = new TreeMap<Integer, Tree.Bucket>();
		
		for(int node : pathSet) { pathMap.put(node, tr.new Bucket(tr.new Block())); }
		
		int failedToEvictCount = 0;
		for (int j = 0; j < union.length; j++) 
		{
			Tree.Block thisBlock = union[j];
			if (thisBlock.isDummy() == false)
			{ 
				Set<Integer> assignedPathSet = getPathString(thisBlock.treeLabel+tr.N-1);
				
				Set<Integer> intersection = new HashSet<Integer>(pathSet);
				intersection.retainAll(assignedPathSet);
				
				int max = Collections.max(intersection);
				
				//System.out.println("\t" + intersection + " -> " + max + "\t(" + assignedPathSet + ")");
				
				Tree.Bucket bucket = pathMap.get(max);
				assert(bucket != null);

				boolean failedToEvict = true;
				for(int k=0; k < Z; k++) 
				{
					if(bucket.blocks[k].isDummy() == true)
					{
						bucket.blocks[k] = tr.new Block(thisBlock);
						thisBlock.erase(); 
						failedToEvict = false;
						break;
					}
				}
				
				if(failedToEvict == true) { failedToEvictCount++; }
			}
		}
		
		//System.out.println("failedToEvictCount: " + failedToEvictCount);
		
		for(int node : pathMap.keySet())
		{		
			Tree.Bucket bucket = pathMap.get(node); assert(bucket != null);
			bucket.encryptBlocks();
		
			String objectKey = recLevel + "#" + node;
			DataItem di = new SimpleDataItem(bucket.toByteArray());
			UploadOperation upload = new UploadOperation(reqID, objectKey, di);
			ScheduledOperation sop = tr.storedTree.uploadObject(upload);
			
			//System.out.print("objectKey: " + objectKey + " ");
			
			v.add(sop);
		}
		Pollable.waitForCompletion(v); v.clear(); // wait and clear
		**/
		
		
		v.clear();
		for (int i = tr.D+1; i > 0; i--) 
		{
			int prefix = Utils.iBitsPrefix(leafLabel+1, tr.D+1, i);
			
			//String objectKey = recLevel + "#" + ((prefix>>(tr.D+1-i))-1);
			
			Tree.Bucket bucket = tr.new Bucket(tr.new Block());
			for (int j = 0, k = 0; j < union.length && k < Z; j++) 
			{
				if (!union[j].isDummy())
				{ 
					int jprefix = Utils.iBitsPrefix(union[j].treeLabel+tr.N, tr.D+1, i);
					
					if(prefix == jprefix) 
					{
						bucket.blocks[k++] = tr.new Block(union[j]);
						
						// debug only
						//{ log.append("[POB (rearrangeBlocksAndReturn)] Block with id " + union[j].id + " will be re-written in bucket on the path to label " + union[j].treeLabel + " (objectKey: " + objectKey + ")", Log.TRACE); }
						
						union[j].erase();
					}
				}
			}
			
			
			/*for(int k=0; k<Z; k++) // debug only
			{
				Tree.Block blk = bucket.blocks[k];
				if(blk.isDummy() == false)
				{
					{ log.append("[POB (rearrangeBlocksAndReturn)] Bucket " + objectKey + " contains block with id " + blk.id + " with label " + blk.treeLabel, Log.TRACE); }
				}
			}*/
			
			bucket.encryptBlocks();
			
			
			int fileLabel = Math.floorDiv(((prefix>>(tr.D+1-i))-1),bucketLimitPerFile);
			String objectKey = Integer.toString(tr.oramID)+ recLevel + Integer.toString(fileLabel);
			int fileOffset = ((prefix>>(tr.D+1-i))-1)%(bucketLimitPerFile);
	
			DataItem di = new SimpleDataItem(bucket.toByteArray());
			UploadObject upload = new UploadObject(reqID, objectKey, di);
			upload.setObjectOffset(fileOffset*(Z*(extDataSize+nonceLen)));
			upload.setObjectSize(Z*(extDataSize+nonceLen));
			ScheduledOperation sop = tr.storedTree.uploadObject(upload);
			
			v.add(sop);
		}
		Pollable.waitForCompletion(v); v.clear(); // wait and clear
		
		
		if(tr.stash.ls == null) { tr.stash.blocks.clear(); } // clear stash in memory
		
		
		// put the rest of the blocks in 'union' into the 'stash'
		int j = 0, k = 0;
		for (; j < union.length && k < tr.stash.size; j++) 
		{
			if (union[j].isDummy() == false) 
			{
					tr.stash.blocks.add(tr.new Block(union[j]));
				
				k++;

				union[j].erase();
			}
		}
		if (k == tr.stash.size) 
		{
			for (; j < union.length; j++)
			{ assert (union[j].isDummy()) : "Stash is overflown: " + tr.stash.size; }	
		}
		
		
		return res;
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

	public Tree.Block read(long reqID, BitSet[] pm, int i) { return access(reqID, pm, OpType.Read, i, null); }
	
	public Tree.Block write(long reqID, BitSet[] pm, int i, BitSet d) { return access(reqID, pm, OpType.Write, i, d); }

	
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