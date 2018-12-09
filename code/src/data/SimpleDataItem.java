package data;
import javax.xml.bind.DatatypeConverter;
public class SimpleDataItem extends DataItem{
protected byte[] data = null;
	
	public SimpleDataItem(byte[] d) { data = d; }
	
	public SimpleDataItem(String fromString)
	{
		data = DatatypeConverter.parseBase64Binary(fromString);
	}
	
	@Override
	public synchronized byte[] getData() { return data; }
}
