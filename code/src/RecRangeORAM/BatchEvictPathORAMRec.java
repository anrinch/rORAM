package RecRangeORAM;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import Interfaces.ExternalStorageInterface;
import pathORAM.PathORAMBasic.Tree.Block;
import pathORAM.PathORAMRec;

public class BatchEvictPathORAMRec extends BatchEvictPathORAMForRec{

private int clientStoreCutoff = 1;
	
	PathORAMRec indexRam = null;
	int rangeSize;
	int oramID;
	
	public BatchEvictPathORAMRec (int cutoff, SecureRandom rng) { super(rng); clientStoreCutoff = cutoff; }
	public BatchEvictPathORAMRec(int cutoff) { this(cutoff, new SecureRandom()); }

	/*
	 * Each data has 'unitB' bytes.
	 */

	public BitSet[] initializeRec(ExternalStorageInterface si, int blocks, int dSize, BitSet[] data, int recLevel, int rangeSize, int oramID, ArrayList<List<Integer>> permutationList) {
		BitSet[] pm = super.initialize(si, blocks, dSize, data, recLevel, rangeSize, oramID, permutationList);
		this.rangeSize = rangeSize;
		this.oramID = oramID;
		if (pm.length <= clientStoreCutoff) {
			indexRam = new PathORAMRec(1,rnd);
			return indexRam.initialize(si, pm.length, (C*serverTree.D + 7)/8, pm, recLevel+1, oramID);
		}
		else {
			indexRam = new PathORAMRec(clientStoreCutoff, rnd);
			return indexRam.initialize(si, pm.length, (C*serverTree.D + 7)/8, pm, recLevel+1, oramID);
		}
	}
	
	protected BatchEvictPathORAMForRec.Tree.Block[] access(long reqId, BitSet[] posMap, BatchEvictPathORAMForRec.OpType op, int a, BitSet data) 
	{
		Tree tr = serverTree;

	
	
		int head = a / C;
		int tail = a % C;
		
		Block indexData = indexRam.read(reqId, posMap, head);
		BitSet chunk = indexData.data;
		int leafLabel =  Utils.readBitSet(chunk, tail*tr.D, tr.D);
		//System.out.println("Reading " + a + " from " + leafLabel + " from " + this.oramID);
		Tree.Block[] blocks = tr.batchReadRange(reqId, leafLabel, a*rangeSize, this.rangeSize);
		assert(blocks[0] != null);		
		
	
		int newlabel = rnd.nextInt(tr.N);
	
		for(int i = 0; i < this.rangeSize; i++ ) {
			blocks[i].treeLabel = (newlabel+i)%tr.N;
			blocks[i].duplicate = -1;
			blocks[i].crmData[oramID] = blocks[i].treeLabel;
		//	System.out.println("Read " + blocks[i].id + " with timestamp " + blocks[i].timeStamp + " from " + leafLabel+ " in ORAM " + oramID);

		}

		
		indexRam.write(reqId, posMap, head, Utils.writeBitSet(chunk, tail*tr.D, newlabel, tr.D));
		
	//	rearrangeBlocksAndReturn(reqId, blocks, rangeSize);
		return blocks;		
				
	
	}
	
	
	
	public void recursiveSave(ObjectOutputStream os) throws IOException
	{		
		super.recursiveSave(os);
		if(indexRam != null) { indexRam.recursiveSave(os); }
	}
	
	public void recursiveLoad(ExternalStorageInterface esi, ObjectInputStream is, int levelsLeft) throws IOException
	{
		if(levelsLeft <= 0) { return; }
		
		initializeLoad(esi, is);
		
		if(levelsLeft == 1)
		{
			indexRam = new PathORAMRec(1,rnd);
		}
		else
		{
			indexRam = new PathORAMRec(clientStoreCutoff, rnd);
		}
		
		indexRam.recursiveLoad(esi, is, levelsLeft - 1);
	}
	
	public int getRecursionLevels() { return 1 + indexRam.getRecursionLevels(); }
	
}
