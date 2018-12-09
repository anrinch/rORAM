package data;

import java.io.ObjectInputStream;

import java.io.ObjectOutputStream;

import utils.Errors;

/**
 * Represents a block header.
 */
public abstract class Header 
{
	protected static Header currentHeader = new DefaultHeader();
	
	public abstract int encodedByteSize();
	public abstract byte[] getEncoding();
	protected abstract void parse(byte[] header);
	
	protected abstract Header create(byte[] headerBytes);
	
	public static void setCurrentHeader(Header header)
	{
		Errors.verify(header != null);
		currentHeader = header;
	}
	
	public static int getByteSize()
	{
		return currentHeader.encodedByteSize();
	}
	
	public static final Header parseHeader(byte[] headerBytes)
	{
		return currentHeader.create(headerBytes);
	}
	
	public static void save(ObjectOutputStream os, Header header) throws Exception
	{
		String name = header.getClass().getSimpleName();
		os.writeObject(name);
		os.writeInt(header.encodedByteSize());
		os.write(header.getEncoding());
	}
	
	public static Header load(ObjectInputStream is) throws Exception
	{
		String simpleName = (String)is.readObject();
		
		int byteSize = is.readInt();
		byte[] b = new byte[byteSize]; is.readFully(b);
		
		if(simpleName.equalsIgnoreCase("DefaultHeader")) { return new DefaultHeader(b); }
				// else if(simpleName.equalsIgnoreCase("New")) { return new NewHeader(b); }
		
		Errors.error("Coding FAIL: unknown header type");
		return null;
	}
}
