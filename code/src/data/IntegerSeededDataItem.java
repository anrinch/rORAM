package data;

import java.util.Random;

/**
 * Represents a data item for which the content is seeded from an integer seed. 
 */
public class IntegerSeededDataItem extends DataItem 
{
	protected int value = 0;
	protected int encodingByteSize = 0;
	protected byte[] data = null;
	
	public IntegerSeededDataItem(int v, int contentByteSize)
	{
		value = v; 
		encodingByteSize = contentByteSize;
		data = null;
	}
	
	@Override
	public synchronized byte[] getData() 
	{
		if(data == null) 
		{
			// seed it with our value, so we get deterministic output
			Random rng = new Random(value); 
			data = new byte[encodingByteSize];
			rng.nextBytes(data);
		}
		
		return data;
	}
}
