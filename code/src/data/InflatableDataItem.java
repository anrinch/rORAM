package data;

import utils.Errors;

/**
 * Represents an inflatable data item. 
 * An inflatable data item is created with a small seed and inflated by calling {@code getData}.
 *
 */
public class InflatableDataItem extends IntegerSeededDataItem 
{
	public static final String stringPrefix = "inflate";
	public InflatableDataItem(int v, int contentByteSize) { super(v, contentByteSize); }

	private static int getValueFromString(String[] s)
	{
		Errors.verify(s[0].equalsIgnoreCase(stringPrefix));
		Errors.verify(s.length == 3);
		return Integer.parseInt(s[2]);
	}
	
	private static int getByteSizeFromString(String[] s)
	{
		return Integer.parseInt(s[1]);
	}
	
	public InflatableDataItem(String fromString) 
	{
		super(getValueFromString(fromString.split("_")), getByteSizeFromString(fromString.split("_")));
	}

	@Override
	public String toString()
	{
		return stringPrefix + "_" + encodingByteSize + "_" + value;
	}
}
