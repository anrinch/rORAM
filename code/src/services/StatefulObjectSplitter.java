package services;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import utils.ClientParameters;
import utils.Errors;

/**
 * Splits objects into blocks.
 *
 */
public class StatefulObjectSplitter 
{
	protected class SplitInfo
	{
		protected String name = null;
		protected int byteSize = 0;
		protected int numBlocks = 0;
		protected int lastBlockByteSize = 0;
		
		public SplitInfo(String n, int bsz, Set<Integer> reclaimedKeys) 
		{
			Errors.verify(n != null && bsz > 0);
			
			name = n;
			byteSize = bsz;
			numBlocks = 0;
			
			while(bsz > 0)
			{
				lastBlockByteSize = Math.min(bsz, blockByteSize);
				bsz -= lastBlockByteSize;
				
				String key = keyFromBlockIdx(name, numBlocks);
				Errors.verify(keysMap.containsKey(key) == false);
				
				int kidx = nextKey;
				if(reclaimedKeys.isEmpty() == true) { nextKey++; }
				else 
				{ 
					Iterator<Integer> iter = reclaimedKeys.iterator();
					kidx = iter.next(); iter.remove();
				}
				keysMap.put(key, kidx);
				
				numBlocks++;
			}
			Errors.verify(numBlocks > 0 && lastBlockByteSize > 0);
			
			int objectSize = (numBlocks - 1) * blockByteSize + lastBlockByteSize;
			Errors.verify(objectSize == byteSize);
		}
	}
	
	protected int nextKey = 0;
	protected Set<Integer> reclaimedKeys = new TreeSet<Integer>();
	
	protected Map<String, Integer> keysMap = new HashMap<String, Integer>();
	protected Map<String, SplitInfo> map = new HashMap<String, SplitInfo>();
	
	protected int blockByteSize = 0;
	
	public StatefulObjectSplitter(int blockSz) { blockByteSize = blockSz; }
	
	private String keyFromBlockIdx(String objName, int blockIdx) { return objName + "--" + blockIdx; }
	
	public void registerObject(String objName, int byteSize)
	{
		// it's ok, if the object is already register, try to update the mapping
		//Errors.verify(map.containsKey(objName) == false, "Object " + objName + " is already registered!");
		
		if(map.containsKey(objName) == true)
		{
			SplitInfo si = map.get(objName);
			if(si.byteSize != byteSize)
			{
				// size is different
				for(int i=0; i<si.numBlocks; i++)
				{
					String key = keyFromBlockIdx(si.name, i);
					Errors.verify(keysMap.containsKey(key) == true);
					int reclaimedKey = keysMap.remove(key);
					reclaimedKeys.add(reclaimedKey);
				}
				
				si = new SplitInfo(objName, byteSize, reclaimedKeys);
				map.put(objName, si);
			}
			
			return;
		}
		
		SplitInfo si = new SplitInfo(objName, byteSize, reclaimedKeys);
		map.put(objName, si);
	}
	
	private String blockKeyFromKeyIdx(int keyIdx) 
	{
		Errors.verify(keyIdx < ClientParameters.getInstance().maxBlocks, "Exceeded the maximum number of blocks!"); 
		return "" + keyIdx;
	}

	public List<Entry<String, Integer>> getKeys(String objName, int startOffset, int endOffset) 
	{
		Errors.verify(map.containsKey(objName) == true, "Object " + objName + " has not been registered!");
		
		Errors.verify(startOffset >= 0 && startOffset <= endOffset);
		List<Entry<String, Integer>> ret = new ArrayList<Entry<String, Integer>>();
		
		SplitInfo si = map.get(objName);
		Errors.verify(si != null && endOffset < si.byteSize);
		
		for(int blockIdx = 0; blockIdx < si.numBlocks; blockIdx++)
		{
			boolean add = true;
			
			int blockSize = ((blockIdx == si.numBlocks-1) ? si.lastBlockByteSize: blockByteSize);
			int blockStartOff = blockIdx * blockByteSize;
			int blockEndOff = blockStartOff + blockSize;
			
			if(blockEndOff < startOffset || blockStartOff > blockEndOff) { add = false; }
			
			if(add == true) 
			{ 
				String key = keyFromBlockIdx(objName, blockIdx);
				Errors.verify(keysMap.containsKey(key) == true);
				int keyIdx = keysMap.get(key);
				ret.add(new AbstractMap.SimpleEntry<String, Integer>(blockKeyFromKeyIdx(keyIdx), blockSize));
			}
		}
		
		return ret;
	}
}
