package RecRangeORAM;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.security.SecureRandom;

public class GenerateRequest {
static SecureRandom rnd;
	
	static {
		try {
			rnd = SecureRandom.getInstance("SHA1PRNG");
			rnd.setSeed(new byte[]{1,2,3,4});
		} catch (Exception e) {
			
		}
	}
	public void generateRequest(int N, int numberOfRequest, String reqFile){
		
		File f = new File(reqFile);
		BufferedWriter b;
		try {
			b = new BufferedWriter(new FileWriter(f));
	
		for(int i = 0; i < numberOfRequest; i++){
			int k1 = rnd.nextInt(N);
			b.write(k1+"\n");
			System.out.println(k1);
			}
		b.close();
		}catch(Exception e){ }
		}
	}
	

