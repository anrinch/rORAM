package data;

import javax.xml.bind.DatatypeConverter;

import utils.Errors;

/**
 * Represents the default block headers. Clients which do not require a special header, use the default header.
 * The default header includes only (a hash of) the block id.
 */
public class DefaultHeader extends Header 
{
	public static final int encodedByteSize = 10;	
	
	protected String hexHash = null;
	
	public DefaultHeader() { }
	
	public DefaultHeader(String h) 
	{
		hexHash = h; 
		Errors.verify(hexHash.length() == 2 * encodedByteSize); 
	}
	
	public DefaultHeader(byte[] h) { parse(h); }
	
	public String toString() { return hexHash; }
	
	@Override
	public int encodedByteSize() { return encodedByteSize; }

	@Override
	public byte[] getEncoding() 
	{
		Errors.verify(hexHash != null);
		byte[] bytes = DatatypeConverter.parseHexBinary(hexHash);
		Errors.verify(bytes.length == encodedByteSize);
		
		return bytes;
	}

	@Override
	protected void parse(byte[] header) 
	{
		Errors.verify(header != null);
		hexHash = DatatypeConverter.printHexBinary(header);
		Errors.verify(hexHash != null);
	}

	@Override
	protected Header create(byte[] header) { return new DefaultHeader(header); }
}
