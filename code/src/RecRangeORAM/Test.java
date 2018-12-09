	package RecRangeORAM;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Interfaces.ExternalStorageInterface;
import RecRangeORAM.BatchEvictPathORAMForRec.Tree.Block;
import backend.LocalStorageSingleFile;
import backend.StorageAdapter;

public class Test {

static SecureRandom rnd;
	
	static {
		try {
			rnd = SecureRandom.getInstance("SHA1PRNG");
			rnd.setSeed(new byte[]{1,2,3,4});
		} catch (Exception e) {
			
		}
	}
		
	public static int getORAM(int rSize, int numberOfORAMs){
		
		return (int) Math.floor(Math.log(rSize)/Math.log(2));
	}
	public static void main(String args[]){

		int useLocal = Integer.parseInt(args[0]);
		int N = Integer.parseInt(args[1]);
		int B = Integer.parseInt(args[2]);
		final int numberOfORAMs = Integer.parseInt(args[3]);
		int iter = Integer.parseInt(args[4]);
		String dataDir = args[5];
	//	String reqFile = args[6];
	//	String reqFile2 = args[7];
		
	
		final BatchEvictPathORAMRec[] CRORAM = new BatchEvictPathORAMRec[numberOfORAMs];
		BitSet[][] pmSet = new BitSet[numberOfORAMs][];
		
		
		// generate data
	
		BitSet[] data = null;
		
		ExternalStorageInterface si = new StorageAdapter(new LocalStorageSingleFile(dataDir, true));
		si.connect();

		
		ArrayList<List<Integer>> permutationList = new ArrayList<List<Integer>>();	
		for(int i = 0; i < numberOfORAMs; i++) {
			List<Integer> permutation = new ArrayList<Integer>();
            for (int j = 0; j < N/Math.pow(2, i); j++) { permutation.add(j); }
            Collections.shuffle(permutation);
            permutationList.add(permutation);
	
		}
		
		for(int i = 0; i < numberOfORAMs; i++) {
			CRORAM[i] = new BatchEvictPathORAMRec(1,rnd);
			pmSet[i] = CRORAM[i].initializeRec(si, N, B, data, 0,(int) Math.pow(2, i),i, permutationList);
		}		

		
				
		int totalNumOfQueries = 0;
		long startTime = System.currentTimeMillis();
		
 		
		for (int i = 0; i < iter; i++) {
			int rSize = 1;//rnd.nextInt(numberOfORAMs);
			int k1 = 0;//rnd.nextInt((int) (N/Math.pow(2,rSize)));
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			BitSet d = BitSet.valueOf(temp);
			System.out.println("Reading " + k1 + " from sub-ORAM " + rSize);
			totalNumOfQueries += Math.pow(2,rSize);
 			
			BatchEvictPathORAMRec.Tree.Block[] b = CRORAM[rSize].read(0l, pmSet[rSize], k1);

			for(int j = 0; j < Math.pow(2,rSize); j++ ) {
				assert (b[j] != null) : "read failed when i = " + i;
		//		b[j].crmData[rSize] = b[j].treeLabel;		//todo: handle it in the backend
			}
			
			
			
			
			for(int k = 0; k < numberOfORAMs; k++) {
				for(int j = 0; j < Math.pow(2,rSize); j++) {
					b[j].treeLabel = b[j].crmData[k];
					//System.out.println("writing " + b[j].id + " with timestamp " + b[j].timeStamp + " to " + b[j].treeLabel + " in ORAM " + k + " " + b[j].crmData[rSize]);
				}
				CRORAM[k].rearrangeBlocksAndReturn(0l, b, (int) Math.pow(2, rSize));
				
			}
		
	}
		
		long endTime = System.currentTimeMillis();
		System.out.println("Random workload: Completed "+ totalNumOfQueries + " queries in " + (endTime-startTime) + " ms -- " + (totalNumOfQueries*1000/(endTime-startTime)) + " queries per second");
		
		
		
		/*
		
		File f_1 = new File(reqFile);
		File f_2 = new File(reqFile2);
		BufferedReader br;
		int totalNumOfQueries = 0;
		long startTime = System.currentTimeMillis();
		
		try{
			br = new BufferedReader(new FileReader(f_1));
			
			for (int i = 0; i < iter; i++) {
				String nextLine = br.readLine();			
				System.out.println(i+": " + nextLine);
				int rSize = Integer.parseInt(nextLine.split(" ")[2])/4;
			final	int oramNum = getORAM(rSize, numberOfORAMs);
			totalNumOfQueries += rSize;	
			int k1 = 1;
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			BitSet d = BitSet.valueOf(temp);
		
			final int rangeSize = (int) Math.pow(2, oramNum);
			final PathORAMBasicBatchEvictRec.Tree.Block[] blks = new PathORAMBasicBatchEvictRec.Tree.Block[rangeSize]; 	
			//System.out.println("read start");
			
			final PathORAMBasicBatchEvictRec.Tree.Block[] b = CRORAM[oramNum].write(0l, pmSet[oramNum], k1,d);
			//System.out.println("check");
			for(int j = 0; j < rangeSize; j++ ) {
				assert (b[j] != null) : "read failed when i = " + i;
				b[j].crmData[numberOfORAMs-1] = b[j].treeLabel;
			}
			
			//System.out.println("read done");
			CRORAM[oramNum].rearrangeBlocksAndReturn(0l, b, rangeSize);

			//System.out.println("batch evict");
			final CountDownLatch latch = new CountDownLatch(numberOfORAMs-1);
			executor.submit(new Runnable(){
				@Override
				public void run(){
			for(int k = 0; k < numberOfORAMs; k++) {
			
				if(k != oramNum){
					for(int j = 0; j < rangeSize; j++) {
					blks[j] = b[j];
					blks[j].treeLabel = b[j].crmData[k];
					blks[j].duplicate = -1;
					}
				CRORAM[k].rearrangeBlocksAndReturn(0l, blks, rangeSize);
				latch.countDown();
				}
			}
				}
			});
			latch.await();
			}
			
		//}
		
			long endTime = System.currentTimeMillis();
			System.out.println("fileserver: Completed "+ totalNumOfQueries + " queries in " + (endTime-startTime) + " ms -- " + (totalNumOfQueries*1000/(endTime-startTime)) + " queries per second");
			
			totalNumOfQueries = 0;
			BufferedReader br1 = new BufferedReader(new FileReader(f_2));
			startTime = System.currentTimeMillis();
			for (int i = 0; i < iter; i++) {
				String nextLine = br1.readLine();			
				System.out.println(nextLine);
				int rSize = Integer.parseInt(nextLine.split(" ")[2])/4;
				int oramNum = getORAM(rSize, numberOfORAMs);
			totalNumOfQueries += rSize;	
			int k1 = 1;
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			BitSet d= BitSet.valueOf(temp);
		
			final int rangeSize = (int) Math.pow(2, oramNum);
			final PathORAMBasicBatchEvictRec.Tree.Block[] blks = new PathORAMBasicBatchEvictRec.Tree.Block[rangeSize]; 	
			final PathORAMBasicBatchEvictRec.Tree.Block[] b = CRORAM[oramNum].write(0l, pmSet[oramNum], k1,d);

			for(int j = 0; j < rangeSize; j++ ) {
				assert (b[j] != null) : "read failed when i = " + i;
				b[j].crmData[numberOfORAMs-1] = b[j].treeLabel;
			}
			
			CRORAM[oramNum].rearrangeBlocksAndReturn(0l, b, rangeSize);

			
			for(int k = 0; k < numberOfORAMs; k++) {
				if(k!=oramNum){
				
				for(int j = 0; j < rangeSize; j++) {
					blks[j] = b[j];
					blks[j].treeLabel = b[j].crmData[k];
					blks[j].duplicate = -1;
					}
				CRORAM[k].rearrangeBlocksAndReturn(0l, blks, rangeSize);
				}
			}
			//});
			}
			//latch.await();
		//}
			endTime = System.currentTimeMillis();
			System.out.println("videoserver: Completed "+ totalNumOfQueries + " queries in " + (endTime-startTime) + " ms -- " + (totalNumOfQueries*1000/(endTime-startTime)) + " queries per second");
			
			br.close();
			br1.close();
			
			
			
		}
		catch(Exception e){}
		*/
		}
		
	}

