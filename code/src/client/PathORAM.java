package client;

import data.EmptyDataItem;

import data.SimpleDataItem;
import data.Header;
import client.AbstractClient;
import pathORAM.PathORAMBasic;
import pathORAM.PathORAMBasic.Tree;
import pathORAM.PathORAMRec;
import services.GetRequest;
import services.PutRequest;
import services.ScheduledRequest;
import utils.Errors;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.*;

public class PathORAM extends AbstractClient{
private BitSet[] posmap = null;
	
	PathORAMBasic oram = null;
	
	public PathORAM() {}
	public PathORAM(SecureRandom r) { rng = r; }

	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{
		// code to restore PathORAM state
		posmap = (BitSet[])is.readObject();
		
		int recLevels = is.readInt();
		
		if(recLevels == 0) { oram = new PathORAMBasic(rng); }
		else { oram = new PathORAMRec(clientParams.localPosMapCutoff, rng); }
		
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


	
	public void init(boolean reset) 
	{
		if(reset == true)
		{
			long maxBlocks = clientParams.maxBlocks;
			
			assert (maxBlocks < Integer.MAX_VALUE) : "ORAM size too large: can't use as an index into an array.";
			if(maxBlocks >= Integer.MAX_VALUE) { Errors.error("ORAM size too large, not supported!"); }
			
			//BitSet[] d = new BitSet[(int)maxBlocks];
			//for (int i = 0; i < maxBlocks; i++) { d[i] = new BitSet(); }
			
			BitSet[] d = null;
			if(maxBlocks <= clientParams.localPosMapCutoff) { oram = new PathORAMBasic(rng); }
			else { oram = new PathORAMRec(clientParams.localPosMapCutoff, rng); }
			
			posmap = oram.initialize(s, (int)maxBlocks, clientParams.contentByteSize, d, 0, 0);
		
		}
	}

	@Override
	public boolean isSynchronous() { return true; } // PathORAM is synchronous


	@Override
	public String getName() { return "PathORAM"; }
	
	@Override
	public ScheduledRequest scheduleGet(GetRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		//try
		{
			Tree.Block res = oram.read(req.getId(), posmap, Integer.parseInt(req.getKey()));
			assert(res != null);
			assert(res.data != null);
			byte[] ret = res.data.toByteArray();
			sreq.onSuccess(new SimpleDataItem(ret));
		} 
		//catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		try
		{
			oram.write(req.getId(), posmap, Integer.parseInt(req.getKey()), BitSet.valueOf(req.getValue().getData()));
			sreq.onSuccess(new EmptyDataItem());
		} 
		catch (Exception e) { sreq.onFailure(); } 
		return sreq;
	}
	
	@Override
	public long peakByteSize() 
	{
		final double bitsPerByte = 8.0;
		
		int entryByteSize = clientParams.contentByteSize + Header.getByteSize();
		long stashSize = PathORAMBasic.stashSize * entryByteSize;
		
		long effectiveN = Math.min(clientParams.maxBlocks, clientParams.localPosMapCutoff);
		
		int logMaxBlocks = (int)Math.ceil(Math.log(effectiveN)/Math.log(2.0));
		int posMapEntrySize = (int)Math.ceil(logMaxBlocks/bitsPerByte);
		long posMapSize = effectiveN * posMapEntrySize;
		
		return stashSize + posMapSize;
	}
}
