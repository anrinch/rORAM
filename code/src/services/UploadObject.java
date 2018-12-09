package services;
import services.Service.ObjectType;
import data.*;

public class UploadObject extends Service{

	protected DataItem dataItem = null;
	private ObjectType objectType = null;
	public UploadObject(long r, String k, DataItem di) { super(r, k); dataItem = di; objectType = ObjectType.Bucket;}
	private int objectOffset;
	private int objectSize;
	
	@Override
	public ServiceType getType() { return ServiceType.UPLOAD; }
	
	public DataItem getDataItem() { return dataItem; }

	@Override
	public ObjectType getObjectType(){ return objectType; }
	public void setObjectType(ObjectType obtype){objectType = obtype;}
	public void setObjectOffset(int offset){ this.objectOffset = offset;}
	public int getObjectOffset(){ return this.objectOffset;}
	public void setObjectSize(int size){ this.objectSize = size;}
	public int getObjectSize(){ return this.objectSize;}
	
}
