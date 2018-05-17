package BatchORAM;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import BatchORAM.RawPathORAM.Tree;
import Interfaces.ExternalStorageInterface;
import backend.LocalStorageSingleFile;
import backend.StorageAdapter;

public class TestRawPathORAM {

	int N = (int) Math.pow(2,  12); //(int) Math.pow(2, 4);
	int B = 16;
	
	static SecureRandom rnd;
	
	static {
		try {
			rnd = SecureRandom.getInstance("SHA1PRNG");
			rnd.setSeed(new byte[]{1,2,3,4});
		} catch (Exception e) {
			
		}
	}

	int iter = N;

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
/*
	@Test
	public void testORAMReads() throws Exception 
	{
		
		int numberOfORAMs = 6;
		
		PathORAMBasicBatchEvict[] CRORAM = new PathORAMBasicBatchEvict[numberOfORAMs];
		BitSet[][] pmSet = new BitSet[numberOfORAMs][];
		
		
		// generate data
				BitSet[] data = new BitSet[N];
				for (int i = 0; i < N; i++) 
				{
					long[] temp = new long[1]; temp[0] = i;
					data[i] = BitSet.valueOf(temp);
				}
				
				ExternalStorageInterface si = new StorageAdapter(new LocalStorageSingleFile("/tmp/Cloud", true));
				si.connect();

				
//				PathORAMBasicBatchEvict oram = new PathORAMBasicBatchEvict(rnd);
//				BitSet[] pm = oram.initialize(si, N, B, data, 0,4,4);

				
		for(int i = 0; i < numberOfORAMs; i++) {
			CRORAM[i] = new PathORAMBasicBatchEvict(rnd);
			pmSet[i] = CRORAM[i].initialize(si, N, B, data, 0,(int) Math.pow(2, i),(int) Math.pow(2, i));
					
		}
		
		int rangeSize =(int) Math.pow(2, (numberOfORAMs-1));
		
		for (int i = 0; i < iter; i++) {
			int k1 = rnd.nextInt(N/rangeSize);
		
		//	PathORAMBasicBatchEvict.Tree.Block[] b = oram.read(0l, pm, k1);
			PathORAMBasicBatchEvict.Tree.Block[] b = CRORAM[numberOfORAMs-1].read(0l, pmSet[numberOfORAMs-1], k1);
			PathORAMBasicBatchEvict.Tree.Block[] blks = new PathORAMBasicBatchEvict.Tree.Block[rangeSize]; 	
			
			
			for(int j = 0; j < rangeSize; j++ )
				assert (b[j] != null) : "read failed when i = " + i;
		
			for(int j = 0; j < numberOfORAMs && j!= (numberOfORAMs -1); j++) {
				CRORAM[j].batchEvict(0l, pmSet[j], b, rangeSize);
				blks = CRORAM[j].read(0l, pmSet[j], rangeSize);
			}
			
			
			//assertEquals(b.data, data[i % N]);
		}
	}
*/

	@Test
	public void testORAMWrites() throws Exception 
	{
		RawPathORAM oram = new RawPathORAM(rnd);
	
		
		
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

		
				
				BitSet[] pm = oram.initialize(si, N, B, data, 0,0);
				
		HashMap<Integer, Tree.Block> blockCache = new HashMap<Integer, Tree.Block>();		
		int cacheSize = 8;	
		
		
		for (int i = 0; i < iter; i++) {

		
			
			int k1 = rnd.nextInt(N);
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			//data[k1] = BitSet.valueOf(temp);
			System.out.println("Reading " + k1);
		
			if(blockCache.containsKey(k1)) {
				System.out.println("---debug");
				
			}
			else {
				RawPathORAM.Tree.Block b = oram.write(0l, pm, k1, BitSet.valueOf(temp));
				assert (b != null) : "read failed when k = " + k1;
				blockCache.put(b.id, b);
			}
			
				
			if(blockCache.keySet().size() == cacheSize) {
				Tree.Block[] blks = new Tree.Block[blockCache.keySet().size()];
				int j = 0;
				for(int key : blockCache.keySet())
					blks[j++] = blockCache.get(key); 
				
				oram.rearrangeBlocksAndReturn(0l, pm, blks, cacheSize);
				blockCache.clear();
			}
			
			//assertEquals("break point: i = "+i, b.data, data[k]);
		}
	}



   /* 
	@Test
	public void testORAMReadsAndWrites() throws Exception 
	{
		PathORAMBasicBatchEvict oram = new PathORAMBasicBatchEvict(rnd);
		
		BitSet[] data = new BitSet[N];
		for (int i = 0; i < N; i++) 
		{
			long[] temp = new long[1]; temp[0] = i;
			data[i] = BitSet.valueOf(temp);
		}

		ExternalStorageInterface si = new StorageAdapter(new LocalStorage("/tmp/Cloud", true));
		si.connect();
		
		
		int rangeSize = 1;
		
		BitSet[] pm_1 = oram.initialize(si, N, B, data, 0,1);
		BitSet[] pm_2 = oram.initialize(si, N, B, data, 0,2);
		
		for (int i = 0; i < iter; i++) {
			int k1 = rnd.nextInt(N/rangeSize);
			PathORAMBasicBatchEvict.Tree.Block[] b = oram.read(0l, pm_2, k1);
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			b.data = BitSet.valueOf(temp);
			

			oram.write(0l, pm_2, k1, b.data);

			data[k1] = b.data;
			
			int k2 = rnd.nextInt(N/rangeSize);
			b = oram.read(0l, pm, k2);
			//assertEquals(b.data, data[k2]);
		}
	}
	
	/*
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
