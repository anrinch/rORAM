package services;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import data.DataItem;
import data.InflatableDataItem;

import utils.ClientParameters;

public class RandomRangeRequestSource extends RequestSource {
	
	public RandomRangeRequestSource(int initLength, int length, int rTestSize)
	{
		this(initLength, length, 0.25, 0.75, rTestSize);
	}
	
	public RandomRangeRequestSource(int initLength, int length, double putProb, double newKeyProb, int rTestSize)
	{
		super();
		
		List<String> putList = randomSequence(initLength, 0.5, 1.0, new ArrayList<String>(), rTestSize);
		if(length > initLength) { randomSequence(length - initLength, putProb, newKeyProb, putList, rTestSize); }
		
		rewind();
	}
	
	private ClientParameters clientParams = ClientParameters.getInstance();

	
	private List<String> randomSequence(int length, double putProb, double newKeyProb, List<String> putList, int rTestSize)
	{	
		Random rng = new SecureRandom();
		Set<String> putSet = new HashSet<String>();
		int nextPutKey = 0;
	
		
		for(int i=0; i < length; i++)
		{		
			final double p1 = putProb;
		
			Request req = null;
			
			String key = null;
			
			nextPutKey = rng.nextInt((int) Math.floorDiv(clientParams.maxBlocks,rTestSize));
			key = "" + nextPutKey;
		
			
			if(putSet.contains(key) == false)
			{
				putSet.add(key);
				putList.add(key);
			}
			
			if(rng.nextDouble() <= p1) // put
			{
				
				DataItem di = new InflatableDataItem(rng.nextInt(), clientParams.contentByteSize);
				req = new PutRequest(key, di);
				req.size = rTestSize;
				
			}
			else // get
			{
				req = new GetRequest(key);
				req.size = rTestSize;
			}
			
			requests.put(req.getId(), req);
		}
		return putList;
	}

}
