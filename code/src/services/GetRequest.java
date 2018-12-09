package services;

import services.Request.RequestType;

public class GetRequest extends Request {
	public GetRequest(String k) { super(k); }

	// default visibility
	public GetRequest(long rid, String k) { super(rid, k); }

	@Override
	public RequestType getType() { return RequestType.GET; }
	
	
	@Override
	public String toString() 
	{
		String ret = super.toString();
		return ret;
	}
}
