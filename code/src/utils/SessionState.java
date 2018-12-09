package utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import services.Request;
public class SessionState {
	private static SessionState instance = null;	
	private SessionState() {}
	
	public static synchronized SessionState getInstance() 
	{ 
		if(instance == null) { instance = new SessionState(); }
		return instance;
	}
	
	private boolean reset = false;
	
	public boolean shouldReset() { return reset;} 
	public void setReset(boolean rst) { reset = rst; }
	
	// state
	protected ClientParameters clientParams = ClientParameters.getInstance();
	
	public boolean debug = false;
	
	public int nextReqId = 0;
	
	public boolean testOnly = true;
	
	public String rsFilePath = null;

	public boolean extracted = false;
	public boolean timing = false;
	public boolean countLogical = false;
	
	public String client = null;
	public String storage = null;
	
	public String storageKey = null;

	public String experimentHash = null;
	
	public boolean fastInit = false;

	public Map<String, Request> fastInitMap = null;
	
	/** load & save order is by order in which the fields appear (maintain this!) **/
	public void save(File sessionFile)
	{
		try
		{
			FileOutputStream fos = new FileOutputStream(sessionFile);
			ObjectOutputStream os = new ObjectOutputStream(fos);
			
			// ----------------------------
			os.writeBoolean(debug);
			
			os.writeInt(nextReqId);
			os.writeBoolean(testOnly);
			
			os.writeObject(rsFilePath);
			os.writeBoolean(extracted);
			os.writeBoolean(timing);
			os.writeBoolean(countLogical);
			
			os.writeObject(client);
			os.writeObject(storage);
			
			os.writeObject(storageKey);
			os.writeObject(experimentHash);
			
			os.writeBoolean(fastInit);
			
			clientParams.save(os);
			
			// ----------------------------
			
			os.flush();
			os.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
	}

	public void load(File sessionFile)
	{
		try
		{
			FileInputStream fis = new FileInputStream(sessionFile);
			ObjectInputStream is = new ObjectInputStream(fis);
			
			// ----------------------------
			debug = is.readBoolean();
			
			nextReqId = is.readInt();
			testOnly = is.readBoolean();
			
			rsFilePath = (String)is.readObject();
			extracted = is.readBoolean();
			timing = is.readBoolean();
			countLogical = is.readBoolean();
			
			client = (String)is.readObject();
			storage = (String)is.readObject();
			
			storageKey = (String)is.readObject();
			experimentHash = (String)is.readObject();
			
			fastInit = is.readBoolean();
			
			clientParams.load(is);
			
			// ----------------------------
			
			is.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
	}
}
