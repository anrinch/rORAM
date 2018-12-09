package data;

public class EmptyDataItem extends DataItem {

public EmptyDataItem() {}
	
	@Override
	public synchronized byte[] getData() { return null; }
}
	

