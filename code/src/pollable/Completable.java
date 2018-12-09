package pollable;
import data.DataItem;

public interface Completable 
{
	public void onSuccess(DataItem d);
	public void onFailure();
}