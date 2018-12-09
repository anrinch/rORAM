package services;

/** 
 * Represents a copy storage operation.
 */
public class CopyObject extends Service 
{
	protected String destKey = null;
	
	public CopyObject(long r, String sk, String dk) { super(r, sk); destKey = dk; }
	
	@Override
	public ServiceType getType() { return ServiceType.COPY; }
	
	public String getDestinationKey() { return destKey; }

	public String getSourceKey() { return getKey(); }

	@Override
	public ObjectType getObjectType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setObjectType(ObjectType obtype) {
		// TODO Auto-generated method stub
		
	}
}
