package utils;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ClientParameters {
	private static ClientParameters instance = null;	
	private ClientParameters() {}
	
	public static synchronized ClientParameters getInstance() 
	{ 
		if(instance == null) { instance = new ClientParameters(); }
		return instance;
	}
	
	public long maxBlocks = 1 << 30;
	
	public int encryptionKeyByteSize = 10;
	public byte[] encryptionKey = null;
	
	public int randomPrefixByteSize = 8; 
	
	public int contentByteSize = 1 << 10;

	public int minContentByteSize = 32;

	public int localPosMapCutoff = 1;

	public boolean noSplit = false;
	
	/** load save order is by order in which the fields appear (maintain this!) **/
	public void save(ObjectOutputStream os) throws Exception
	{
		os.writeLong(maxBlocks);
		os.writeInt(encryptionKeyByteSize);
		
		assert(encryptionKey != null && encryptionKey.length == encryptionKeyByteSize);
		os.write(encryptionKey);
		
		os.writeInt(randomPrefixByteSize);
		
		os.writeInt(contentByteSize);
		os.writeInt(minContentByteSize);
		
		os.writeInt(localPosMapCutoff);
		
		os.writeBoolean(noSplit);
	}

	public void load(ObjectInputStream is) throws Exception
	{
		maxBlocks = is.readLong();
		
		encryptionKeyByteSize = is.readInt();
		
		encryptionKey = new byte[encryptionKeyByteSize];
		is.readFully(encryptionKey);
		
		randomPrefixByteSize = is.readInt();
		
		contentByteSize = is.readInt();
		minContentByteSize = is.readInt();
		
		localPosMapCutoff = is.readInt();
		
		noSplit = is.readBoolean();
	}
}
