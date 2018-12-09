package Construction;

import Interfaces.ExternalStorageInterface;
import Interfaces.InternalStorageInterface;
import client.PathORAM;
import client.RAMClient;
import client.RangeORAMRec;
import client.BatchedPathORAMClient;
import data.DefaultHeader;
import data.Header;
import backend.*;
import Interfaces.*;
import utils.Errors;

public class SchemeFactory {
	private static SchemeFactory instance = null;	
	private SchemeFactory() {}
	
	public static synchronized SchemeFactory getInstance() 
	{ 
		if(instance == null) { instance = new SchemeFactory(); }
		return instance;
	}
	
	protected String[] parseDesc(String desc)
	{
		String d = null; String[] s = null;
	
		/*
		int pos = desc.indexOf("(");
		if(pos >= 0)
		{
			d = desc.substring(0, pos);
			String param = desc.substring(pos+1);
			param = param.substring(0, param.indexOf(")")); 
			s = param.split(",\\s*");
			desc = desc.substring(0, desc.indexOf("("));
		}
		else { d = desc; }
		
		String[] ret = new String[1 + ((s == null) ? 0 : s.length)];
		ret[0] = d; if(s != null) { for(int i=0; i<s.length; i++) { ret[1+i] = s[i]; } }
		*/
		d = desc;
		String[] ret = d.split(" ");
		
		return ret;
	}
	
	public ExternalStorageInterface createStorage(String storage, boolean reset)
	{
		String[] storageDesc = parseDesc(storage);
		
		InternalStorageInterface s = null;
		switch(storageDesc[0])
		{
		case "LocalStorageSingleFile":
			if(storageDesc.length > 1) 
			{
				Errors.verify(storageDesc.length == 2);
				s = new LocalStorageSingleFile(storageDesc[1], reset); 
			}
			else { s = new LocalStorageSingleFile(reset); }
			break;

		case "SyncLocal": 
			if(storageDesc.length > 1) 
			{
				Errors.verify(storageDesc.length == 2);
				s = new LocalStorage(storageDesc[1], reset); 
			}
			else { s = new LocalStorage(reset); }
			break;
		
		case "AsyncLocal":
			if(storageDesc.length > 2)
			{
				Errors.verify(storageDesc.length == 3);
				s = new AsyncLocalStorage(storageDesc[1], reset, Integer.parseInt(storageDesc[2])); 
			}
			else if(storageDesc.length > 1) 
			{
				Errors.verify(storageDesc.length == 2);
				s = new AsyncLocalStorage(storageDesc[1], reset); 
			}
			else { s = new AsyncLocalStorage(reset); }
			break;
		case "S3": s = new AmazonS3Storage(reset); break;
		case "AsyncS3": s = new AmazonS3AsyncStorage(reset); break;
		default: 
			Errors.error("Unknown storage: " + storage);
		}
		
		return new StorageAdapter(s);
	}
	
	public ExternalClientInterface createClient(String client)
	{		
		String[] clientDesc = parseDesc(client);
		
		InternalClientInterface c = null;
		
		switch(clientDesc[0])
		{
		
		case "RAMClient": 
			if(clientDesc.length > 1)  
			{
				Errors.verify(clientDesc.length == 2);
				c = new RAMClient(Boolean.parseBoolean(clientDesc[1])); 
			}
			else { c = new RAMClient(); }
			break;
		
	
		case "PathORAM": 
			assert(clientDesc.length == 1);
			c = new PathORAM(); 
			break;
		
		case "RawPathORAM":
			assert(clientDesc.length == 1);
			c = new BatchedPathORAMClient(); 
			break;
			
		
		case "RangeORAMRec":
			assert(clientDesc.length == 2);
			c = new RangeORAMRec(Integer.parseInt(clientDesc[1])); 
			break;
			
			
		default: 
			Errors.error("Unknown client: " + client);
		}
		
		return new ClientAdapter(c);
	}
	
	public void setHeader(String client)
	{
		String[] clientDesc = parseDesc(client);
			Header.setCurrentHeader(new DefaultHeader());
		
	}
}
