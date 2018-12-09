package services;

public class DeleteObject extends Service{

public DeleteObject(long r, String k) { super(r, k); }
	
	@Override
	public ServiceType getType() { return ServiceType.DELETE; }

	@Override
	public ObjectType getObjectType() { return null; }

	@Override
	public void setObjectType(ObjectType obtype) {
		;
	}

}


