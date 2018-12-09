package pathORAM;
import Interfaces.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.*;


public class PathORAMRec extends PathORAMBasic
{
	private int clientStoreCutoff = 1000;
	
	PathORAMBasic indexRam = null;

	public PathORAMRec (int cutoff, SecureRandom rng) { super(rng); clientStoreCutoff = cutoff; }
	
	public PathORAMRec(int cutoff) { this(cutoff, new SecureRandom()); }

	/*
	 * Each data has 'unitB' bytes.
	 */
	public BitSet[] initialize(ExternalStorageInterface si, int blocks, int dSize, BitSet[] data, int recLevel, int oramID) {
		BitSet[] pm = super.initialize(si, blocks, dSize, data, recLevel, oramID);
		if (pm.length <= clientStoreCutoff) {
			indexRam = new PathORAMBasic(rnd);
			return indexRam.initialize(si, pm.length, (C*serverTree.D + 7)/8, pm, recLevel+1, oramID);
		}
		else {
			indexRam = new PathORAMRec(clientStoreCutoff, rnd);
			return indexRam.initialize(si, pm.length, (C*serverTree.D + 7)/8, pm, recLevel+1, oramID);
		}
	}

	protected PathORAMBasic.Tree.Block access(long reqId, BitSet[] posMap, PathORAMBasic.OpType op, int a, BitSet data) 
	{
		Tree tr = serverTree;
		int head = a / C;
		int tail = a % C;
		BitSet chunk = indexRam.read(reqId, posMap, head).data;
		
		int leafLabel = tr.N-1 + Utils.readBitSet(chunk, tail*tr.D, tr.D);
		Tree.Block[] blocks = tr.readBuckets(reqId, leafLabel);
		
		int newlabel = rnd.nextInt(tr.N);
		indexRam.write(reqId, posMap, head, Utils.writeBitSet(chunk, tail*tr.D, newlabel, tr.D));

		return rearrangeBlocksAndReturn(reqId, op, a, data, leafLabel, blocks, newlabel);
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
			indexRam = new PathORAMBasic(rnd);
		}
		else
		{
			indexRam = new PathORAMRec(clientStoreCutoff, rnd);
		}
		
		indexRam.recursiveLoad(esi, is, levelsLeft - 1);
	}
	
	public int getRecursionLevels() { return 1 + indexRam.getRecursionLevels(); }
}