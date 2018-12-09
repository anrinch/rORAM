package client;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import RecRangeORAM.BatchedPathORAM;
import RecRangeORAM.BatchedPathORAM.Tree;
import data.EmptyDataItem;
import data.Header;
import data.SimpleDataItem;
import services.GetRequest;
import services.PutRequest;
import services.ScheduledRequest;
import utils.Errors;

public class BatchedPathORAMClient extends AbstractClient{

	
private BitSet[] posmap = null;
	
	BatchedPathORAM oram = null;
	
	public BatchedPathORAMClient() {}
	public BatchedPathORAMClient(SecureRandom r) { rng = r; }

	HashMap<Integer, Tree.Block> cache = new HashMap<Integer, Tree.Block>();
	int maxCacheSize=8;
	
	
	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{
		// code to restore PathORAM state
		posmap = (BitSet[])is.readObject();
		
		int recLevels = is.readInt();
		
		if(recLevels == 0) { oram = new BatchedPathORAM(rng); }
	//	else { oram = new PathORAMRec(clientParams.localPosMapCutoff, rng); }
		
		oram.recursiveLoad(s, is, recLevels);
	}

	@Override
	protected void save(ObjectOutputStream os) throws Exception 
	{
		// code to save PathORAM state
		os.writeObject(posmap);
		
		int recLevels = oram.getRecursionLevels();
		os.writeInt(recLevels);
		
		oram.recursiveSave(os);
	}


	
	protected void init(boolean reset) 
	{
		if(reset == true)
		{		
			long maxBlocks = clientParams.maxBlocks;
			
			assert (maxBlocks < Integer.MAX_VALUE) : "ORAM size too large: can't use as an index into an array.";
			if(maxBlocks >= Integer.MAX_VALUE) { Errors.error("ORAM size too large, not supported!"); }
			
			BitSet[] d = null; 
			oram = new BatchedPathORAM(rng);
			ArrayList<List<Integer>> permutationList = new ArrayList<List<Integer>>();	
			
			List<Integer> permutation = new ArrayList<Integer>();
	        	for (int j = 0; j < maxBlocks; j++) { permutation.add(j); }
	        	Collections.shuffle(permutation);
	        	permutationList.add(permutation);

	        	posmap = oram.initialize(s, (int)maxBlocks, clientParams.contentByteSize, d, 0, 0);
		}
	}

	@Override
	public boolean isSynchronous() { return true; } // PathORAM is synchronous


	@Override
	public String getName() { return "PathORAMBatchEvict"; }
	
	@Override
	public ScheduledRequest scheduleGet(GetRequest req) 
	{
		System.out.println("GET: requesting block " + "at address " + Integer.parseInt(req.getKey()));
		ScheduledRequest sreq = new ScheduledRequest(req);
		try
		{
			if(cache.containsKey(Integer.parseInt(req.getKey()))) {
				byte[] ret = cache.get(req.getKey()).data.toByteArray();
				sreq.onSuccess(new SimpleDataItem(ret));
			}
			else {
				Tree.Block blk= oram.read(req.getId(), posmap, Integer.parseInt(req.getKey()));
				assert(blk != null);
				cache.put(Integer.parseInt(req.getKey()), blk);
				byte[] ret = blk.data.toByteArray();
				sreq.onSuccess(new SimpleDataItem(ret));
			}
			
			if(cache.keySet().size() == maxCacheSize) {
				int ctr = 0;
				Tree.Block[] blks = new Tree.Block[maxCacheSize];
				for(int key: cache.keySet()) {
					blks[ctr++] = oram.serverTree.new Block(cache.get(key));
				}
				oram.rearrangeBlocksAndReturn(0l, posmap, blks, maxCacheSize);	
				cache.clear();
			}
		 
	
			
		}
		
		catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		System.out.println("PUT: requesting block " + "at address " + Integer.parseInt(req.getKey()));

		try
		{
			if(cache.containsKey(Integer.parseInt(req.getKey()))) {
				byte[] ret = cache.get(req.getKey()).data.toByteArray();
				cache.get(Integer.parseInt(req.getKey())).data = BitSet.valueOf(req.getValue().getData());
				sreq.onSuccess(new EmptyDataItem());
			}
			else {
				Tree.Block blk= oram.read(req.getId(), posmap, Integer.parseInt(req.getKey()));
				assert(blk != null);
				cache.put(Integer.parseInt(req.getKey()), blk);
				sreq.onSuccess(new EmptyDataItem());
			}
			if(cache.keySet().size() == maxCacheSize) {
				int ctr = 0;
				Tree.Block[] blks = new Tree.Block[maxCacheSize];
				for(int key: cache.keySet()) {
					blks[ctr++] = oram.serverTree.new Block(cache.get(key));
				}
				oram.rearrangeBlocksAndReturn(0l, posmap, blks, maxCacheSize);	
				cache.clear();
			}
		} 
		
		catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	
	@Override
	public long peakByteSize() 
	{
		final double bitsPerByte = 8.0;
		int entryByteSize = clientParams.contentByteSize + Header.getByteSize();
		long stashSize = BatchedPathORAM.stashSize * entryByteSize;
		long effectiveN = Math.min(clientParams.maxBlocks, clientParams.localPosMapCutoff);
		int logMaxBlocks = (int)Math.ceil(Math.log(effectiveN)/Math.log(2.0));
		int posMapEntrySize = (int)Math.ceil(logMaxBlocks/bitsPerByte);
		long posMapSize = effectiveN * posMapEntrySize;
		
		return stashSize + posMapSize;
	}
	
	
	
}
