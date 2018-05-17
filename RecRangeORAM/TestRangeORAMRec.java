package RecRangeORAM;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import Interfaces.ExternalStorageInterface;
import RecRangeORAM.PathORAMBasicForRec.Tree.Block;
import backend.LocalStorageSingleFile;
import backend.StorageAdapter;

public class TestRangeORAMRec {

	int N = (int) Math.pow(2,  6); //(int) Math.pow(2, 4);
	int B = 16;
	
	static SecureRandom rnd;
	
	static {
		try {
			rnd = SecureRandom.getInstance("SHA1PRNG");
			rnd.setSeed(new byte[]{1,2,3,4});
		} catch (Exception e) {
			
		}
	}

	int iter = 200;

    @BeforeClass
    public static void oneTimeSetUp() {
        // one-time initialization code   
    	System.out.println("@BeforeClass - oneTimeSetUp");
    	
    	
    }
 
    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
    	System.out.println("@AfterClass - oneTimeTearDown");
    	
    	
    }

	@Test
	public void testORAMReads() throws Exception 
	{
	
		
		//PathORAMBasicBatchEvictRec oram = new PathORAMBasicBatchEvictRec(1,rnd);
		
		
		int numberOfORAMs = 4;
		
		PathORAMBasicBatchEvictRec[] CRORAM = new PathORAMBasicBatchEvictRec[numberOfORAMs];
		BitSet[][] pmSet = new BitSet[numberOfORAMs][];
		
		
		// generate data
	
		BitSet[] data = null;
/*
		BitSet[] data = new BitSet[N];
		for (int i = 0; i < N; i++) 
		{
			long[] temp = new long[1]; temp[0] = i;
			data[i] = BitSet.valueOf(temp);

		}
	*/	
		ExternalStorageInterface si = new StorageAdapter(new LocalStorageSingleFile("/tmp/Cloud", true));
		si.connect();

		
		ArrayList<List<Integer>> permutationList = new ArrayList<List<Integer>>();	
		for(int i = 0; i < numberOfORAMs; i++) {
			List<Integer> permutation = new ArrayList<Integer>();
            for (int j = 0; j < N/Math.pow(2, i); j++) { permutation.add(j); }
            Collections.shuffle(permutation);
            permutationList.add(permutation);
	
		}
		
		for(int i = 0; i < numberOfORAMs; i++) {
			CRORAM[i] = new PathORAMBasicBatchEvictRec(1,rnd);
			pmSet[i] = CRORAM[i].initializeRec(si, N, B, data, 0,(int) Math.pow(2, i),i, permutationList);
		}		
		

		int rangeSize =(int) Math.pow(2, (numberOfORAMs-1));
 		
for (int i = 0; i < iter; i++) {
	int k1 = rnd.nextInt(N/rangeSize);
	byte[] temp = new byte[B];
	rnd.nextBytes(temp);
	//data[k1] = BitSet.valueOf(temp);
	System.out.println("Reading " + k1);

	PathORAMBasicBatchEvictRec.Tree.Block[] blks = new PathORAMBasicBatchEvictRec.Tree.Block[rangeSize]; 	
	PathORAMBasicBatchEvictRec.Tree.Block[] b = CRORAM[numberOfORAMs-1].read(0l, pmSet[numberOfORAMs-1], k1);

	for(int j = 0; j < rangeSize; j++ ) {
		assert (b[j] != null) : "read failed when i = " + i;
		b[j].crmData[numberOfORAMs-1] = b[j].treeLabel;
	}
	
	CRORAM[numberOfORAMs-1].rearrangeBlocksAndReturn(0l, b, rangeSize);

	
	
	for(int k = 0; k < numberOfORAMs -1; k++) {
		for(int j = 0; j < rangeSize; j++) {
			blks[j] = b[j];
			blks[j].treeLabel = b[j].crmData[k];
			blks[j].duplicate = -1;
		}
		CRORAM[k].rearrangeBlocksAndReturn(0l, blks, rangeSize);
		
	}
	
	
	
//	assert (b != null) : "read failed when k = " + k;
	//assertEquals("break point: i = "+i, b.data, data[k]);
}
		
		
	}
		/*	
	@Test
	public void testORAMWrites() throws Exception 
	{
		System.out.println("N = " + N);
		PathORAMBasic oram = new PathORAMBasic(rnd);
		
		BitSet[] data = new BitSet[N];
		for (int i = 0; i < N; i++) 
		{
			long[] temp = new long[1]; temp[0] = i;
			data[i] = BitSet.valueOf(temp);
		}
		
		ExternalStorageInterface si = new StorageAdapter(new LocalStorage("/tmp/Cloud", true));
		si.connect();;
		BitSet[] pm = oram.initialize(si, N, B, data, 0);
//		BitSet[] pm = oram.initialize(N, B);

		for (int i = 0; i < iter; i++) {
			int k = rnd.nextInt(N);
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			data[k] = BitSet.valueOf(temp);

			oram.write(0l, pm, k, data[k]);
			PathORAMBasic.Tree.Block b = oram.read(0l, pm, k);
			assert (b != null) : "read failed when k = " + k;
			assertEquals("break point: i = "+i, b.data, data[k]);
		}
	}
	
	@Test
	public void testORAMReadsAndWrites() throws Exception 
	{
		PathORAMBasic oram = new PathORAMBasic(rnd);
		
		BitSet[] data = new BitSet[N];
		for (int i = 0; i < N; i++) 
		{
			long[] temp = new long[1]; temp[0] = i;
			data[i] = BitSet.valueOf(temp);
		}

		ExternalStorageInterface si = new StorageAdapter(new LocalStorage("/tmp/Cloud", true));
		si.connect();
		BitSet[] pm = oram.initialize(si, N, B, data, 0);
		
		for (int i = 0; i < iter; i++) {
			int k1 = rnd.nextInt(N);
			PathORAMBasic.Tree.Block b = oram.read(0l, pm, k1);
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			b.data = BitSet.valueOf(temp);
			

			oram.write(0l, pm, k1, b.data);

			data[k1] = b.data;
			
			int k2 = rnd.nextInt(N);
			b = oram.read(0l, pm, k2);
			assertEquals(b.data, data[k2]);
		}
	}
	
	@Test
	public void testAllOnVariousN() throws Exception 
	{
		for (int i = 8; i < 12; i++) {
			N = (int)Math.pow(2, i);
			testORAMReads();
			testORAMWrites();
			testORAMReadsAndWrites();
		}
	}
	*/
	
}
