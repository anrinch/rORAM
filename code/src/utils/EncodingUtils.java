package utils;
import java.util.AbstractMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;
import utils.ClientParameters;
import data.Header;


public class EncodingUtils {
private static final EncodingUtils instance = new EncodingUtils();
	
	private EncodingUtils() {}
	
	public static EncodingUtils getInstance() { return instance; }
	
	private ClientParameters clientParams = ClientParameters.getInstance();
	
	
	public byte[] encodeBlock(Header header, byte[] objectPayload)
	{
		int headerByteSize = header.encodedByteSize();
		int encodedByteSize = headerByteSize + objectPayload.length;
		
		byte[] ret = new byte[encodedByteSize];
		
		byte[] headerBytes = header.getEncoding();
		
		System.arraycopy(headerBytes, 0, ret, 0, headerByteSize);
		System.arraycopy(objectPayload, 0, ret, headerByteSize, objectPayload.length);
		
		// note: the first rByteSize bytes of data are zeros!
		
		return ret;
	}
	
	public Map.Entry<Header, byte[]> decodeBlock(byte[] encodedData)
	{
		int headerByteSize = Header.getByteSize();
	
		byte[] headerBytes = new byte[headerByteSize];
		System.arraycopy(encodedData, 0, headerBytes, 0, headerByteSize);
		
		Header header = Header.parseHeader(headerBytes);
		
		int payloadByteSize = encodedData.length - headerByteSize;
		byte[] objectPayload = new byte[payloadByteSize];
		System.arraycopy(encodedData, headerByteSize, objectPayload, 0, objectPayload.length);
	
		return new AbstractMap.SimpleEntry<Header, byte[]>(header, objectPayload);
	}

	public String toHexString(byte[] data) { return DatatypeConverter.printHexBinary(data); }

	public byte[] getDummyBytes(int size) { return new byte[size]; }
}
