package services;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import utils.ClientParameters;
import data.DataItem;
import data.InflatableDataItem;
import services.*;

/**
 * Implements a request source of random requests.
 * This class can be used to generate random requests.
 */
public class RandomRequestSource extends RequestSource
{	
	public RandomRequestSource(int initLength, int length)
	{
		this(initLength, length, 0.25, 0.75);
	}
	
	public RandomRequestSource(int initLength, int length, double putProb, double newKeyProb)
	{
		super();
		
		List<String> putList = randomSequence(initLength, 0.5, 1.0, new ArrayList<String>());
		if(length > initLength) { randomSequence(length - initLength, putProb, newKeyProb, putList); }
		
		rewind();
	}
	
	private ClientParameters clientParams = ClientParameters.getInstance();
	
	private List<String> randomSequence(int length, double putProb, double newKeyProb, List<String> putList)
	{	
		Random rng = new SecureRandom();
		
		Set<String> putSet = new HashSet<String>();
		int nextPutKey = 0;
		
		for(int i=0; i < length; i++)
		{		
			final double p1 = putProb;
			
			Request req = null;
			
			String key = null;
			
			nextPutKey = rng.nextInt((int) clientParams.maxBlocks);
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
				
			}
			else // get
			{
				req = new GetRequest(key);
			}
			
			requests.put(req.getId(), req);
		}
		return putList;
	}
}
