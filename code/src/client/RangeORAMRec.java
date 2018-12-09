package client;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import RecRangeORAM.BatchEvictPathORAMRec;
import RecRangeORAM.BatchEvictPathORAMForRec.Tree;
import data.EmptyDataItem;
import data.Header;
import data.SimpleDataItem;
import services.GetRequest;
import services.PutRequest;
import services.ScheduledRequest;
import utils.Errors;

public class RangeORAMRec extends AbstractClient{

	int numberOfORAMs;
	BatchEvictPathORAMRec[] CRORAM;
	BitSet[][] pmSet;
	
BatchEvictPathORAMRec oram = null;
	
	public RangeORAMRec(int numberOfORAMs) {this.numberOfORAMs = numberOfORAMs;}
	public RangeORAMRec(SecureRandom r) { rng = r; }

	
	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{
		int recLevels = is.readInt();
		if(recLevels == 0) { oram = new BatchEvictPathORAMRec(1,rng); }
	
	}

	@Override
	protected void save(ObjectOutputStream os) throws Exception 
	{
		;
	}


	
	protected void init(boolean reset) 
	{
		if(reset == true)
		{		
			long maxBlocks = clientParams.maxBlocks;
			CRORAM = new BatchEvictPathORAMRec[numberOfORAMs];
			pmSet = new BitSet[numberOfORAMs][];
			assert (maxBlocks < Integer.MAX_VALUE) : "ORAM size too large: can't use as an index into an array.";
			if(maxBlocks >= Integer.MAX_VALUE) { Errors.error("ORAM size too large, not supported!"); }
			BitSet[] d = null;
			ArrayList<List<Integer>> permutationList = new ArrayList<List<Integer>>();	
			for(int i = 0; i < numberOfORAMs; i++) {
				List<Integer> permutation = new ArrayList<Integer>();
	            for (int j = 0; j < maxBlocks/Math.pow(2, i); j++) { permutation.add(j); }
	            Collections.shuffle(permutation);
	            permutationList.add(permutation);
			}
			
			for(int i = 0; i < numberOfORAMs; i++) {
				CRORAM[i] = new BatchEvictPathORAMRec(1,rng);
				pmSet[i] = CRORAM[i].initializeRec(s, (int) maxBlocks, clientParams.contentByteSize, d, 0,(int) Math.pow(2, i),i, permutationList);
			}
		}
	}

	@Override
	public boolean isSynchronous() { return true; } // PathORAM is synchronous


	@Override
	public String getName() { return "PathORAMBatchEvictRec"; }
	
	@Override
	public ScheduledRequest scheduleGet(GetRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		int oramID =0;
		int rSize = 0;
		int rStart=0;
		
		for (int i = 0; i <= numberOfORAMs; i++){
			if(Math.pow(2, i) > req.size){
				oramID = i-1;
				rSize =  (int) Math.pow(2,oramID);
				rStart = (int)  Math.floorDiv(Integer.parseInt(req.getKey()),(int) Math.pow(2, oramID));
				}
			}
		System.out.println("GET: requesting range of size " + req.size + " at address " + rStart);
		BatchEvictPathORAMRec.Tree.Block[] b1 = CRORAM[oramID].read(0l, pmSet[oramID], rStart);
		BatchEvictPathORAMRec.Tree.Block[] b2 = CRORAM[oramID].read(0l, pmSet[oramID], rStart+1);
			
		assert(b2 != null);
		assert(b2[0].data != null);
		byte[] ret = b2[0].data.toByteArray();
		
		/* Batch evicting to all ORAMs */
		for(int k = 0; k < numberOfORAMs; k++) {
			for(int j = 0; j < rSize; j++) {
				b1[j].treeLabel = b1[j].crmData[k];
				b2[j].treeLabel = b2[j].crmData[k];
			}
			CRORAM[k].rearrangeBlocksAndReturn(0l, b1, rSize);
			CRORAM[k].rearrangeBlocksAndReturn(0l, b2, rSize);
		}
		sreq.onSuccess(new SimpleDataItem(ret));
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		try
		{
			int oramID =0;
			int rSize = 0;
			int rStart=0;
			for (int i = 0; i <= numberOfORAMs; i++){
			   	if(Math.pow(2, i) > req.size){
					oramID = i-1;
					rSize =  (int) Math.pow(2,oramID);
					rStart = (int)  Math.floorDiv(Integer.parseInt(req.getKey()),(int) Math.pow(2, oramID));
				}
			}
			System.out.println("PUT: requesting range of size " + req.size + " at address " + rStart);
			BatchEvictPathORAMRec.Tree.Block[] b1 = CRORAM[oramID].write(0l, pmSet[oramID], rStart, BitSet.valueOf(req.getValue().getData()));
			BatchEvictPathORAMRec.Tree.Block[] b2 = CRORAM[oramID].write(0l, pmSet[oramID], rStart+1, BitSet.valueOf(req.getValue().getData()));
			sreq.onSuccess(new EmptyDataItem());
			
			/* Batch evict to all ORAMs */
			for(int k = 0; k < numberOfORAMs; k++) {
				for(int j = 0; j < rSize; j++) {
					b1[j].treeLabel = b1[j].crmData[k];
					b2[j].treeLabel = b2[j].crmData[k];
				}
				CRORAM[k].rearrangeBlocksAndReturn(0l, b1, rSize);
				CRORAM[k].rearrangeBlocksAndReturn(0l, b2, rSize);
			}
		}
		catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	
	
	
	public static int getORAM(long len) {
		
		int i = 0; 
		while(Math.pow(2, i) < len)
			i++;
	
		return i;
	}
	
	@Override
	public long peakByteSize() 
	{
		final double bitsPerByte = 8.0;
		
		int entryByteSize = clientParams.contentByteSize + Header.getByteSize();
		long stashSize = BatchEvictPathORAMRec.stashSize * entryByteSize;
		long effectiveN = Math.min(clientParams.maxBlocks, clientParams.localPosMapCutoff);
		int logMaxBlocks = (int)Math.ceil(Math.log(effectiveN)/Math.log(2.0));
		int posMapEntrySize = (int)Math.ceil(logMaxBlocks/bitsPerByte);
		long posMapSize = effectiveN * posMapEntrySize;
		
		return stashSize + posMapSize;
	}
	
}
