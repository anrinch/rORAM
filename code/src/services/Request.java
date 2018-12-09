package services;
import java.util.concurrent.atomic.AtomicLong;
import data.DataItem;
import data.InflatableDataItem;
import data.SimpleDataItem;
import utils.Errors;

public abstract class Request implements Comparable<Request>{
public enum RequestType { GET, PUT };
	
	public int compareTo(Request anotherReq) 
	{
        if(this.reqId < anotherReq.reqId) { return -1; }
        else if(this.reqId > anotherReq.reqId) { return 1; }
        
        return 0;
    }
	public int size = 0;
	public static long initReqId = 0;
	
	private static AtomicLong nextReqId = new AtomicLong(1);
	
	protected long reqId = 0;
	protected String key = null;
	
	protected LogicalRequest tag = null;
	protected long timestamp = -1;
	
	public Request(String k) { key = k; reqId = nextReqId.getAndIncrement(); }
	
	Request(long rid, String k) { reqId = rid; key = k; }
	
	public abstract RequestType getType();
	
	public long getId() { return reqId; }
	public String getKey() { return key; }
	
	public String getStringDesc()
	{
		String name = "";
		if(tag != null) 
		{ 
			String tname = tag.getName();
			if(getKey().equalsIgnoreCase(tname) == false) { name = "name: " + tname + ", "; }
		}
		return "Req(type: " + getType().toString() + ", " + name + "id: " + getId() + ", key: \"" + getKey() + "\")";
	}
	
	public String toString()
	{
		return getType().toString() + ", " + getId() + ", " + getKey();
	}

	public static Request fromString(String fromString)
	{
		String[] s = fromString.split(", ");
		if(s.length < 3) { Errors.error("Invalid fromString: " + fromString); }
		switch(s[0])
		{
		case "GET": return new GetRequest(Long.parseLong(s[1]), s[2]);
		case "PUT": if(s.length < 4) { Errors.error("Invalid fromString (PUT): " + fromString);	}
				DataItem di = null; String diFromString = s[3];
				if(diFromString.startsWith(InflatableDataItem.stringPrefix)) { di = new InflatableDataItem(diFromString); } 
				else { di = new SimpleDataItem(diFromString); }
				return new PutRequest(Long.parseLong(s[1]), s[2], di);
		default:
			Errors.error("Invalid fromString: " + fromString); return null;
		}
	}
	
	public void setTag(LogicalRequest t) { tag = t; }
	public LogicalRequest getTag() { return tag; }
	
	public void setTimestamp(long ts) { timestamp = ts; }
	public long getTimestamp() { return timestamp; }
	
	public static GetRequest newAssociatedGetRequest(PutRequest req, String key2) 
	{
		return new GetRequest(req.getId(), key2);
	}
	
}
