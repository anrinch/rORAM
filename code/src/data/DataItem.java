package data;

import javax.xml.bind.DatatypeConverter;

public abstract class DataItem {

public abstract byte[] getData();
	
	@Override
	public synchronized String toString()
	{
		return DatatypeConverter.printBase64Binary(getData());
	}
}
