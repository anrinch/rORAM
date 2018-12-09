package data;

import java.io.Serializable;

public class DummyBlock extends DataItem implements Serializable{

	private byte[] payload;
	
	public DummyBlock() {payload = new byte[4096];}
	
	@Override
	public byte[] getData() {
		return null;
	}

	
}
