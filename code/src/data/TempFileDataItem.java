package data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import utils.SystemParameters;

/**
 * Represents a data item backed by a temporary file.
 *
 */
public class TempFileDataItem extends DataItem
{
	protected File tempFile = null;
	
	protected byte[] data = null;
	
	public TempFileDataItem()
	{
		File dir = new File(SystemParameters.getInstance().tempDirectoryFP); 
		if(dir.exists() == false) { dir.mkdir(); }
		try { tempFile = File.createTempFile("CloudExp__", ".temp.dataitem", dir); } 
		catch (IOException e) { throw new RuntimeException(e); }
		
		data = null;
	}
	
	@Override
	public synchronized byte[] getData() 
	{
		if(data == null)
		{
			assert(tempFile != null);
			try 
			{
				tempFile = tempFile.getAbsoluteFile();
				data = new byte[(int)tempFile.length()];
				
				FileInputStream fi = new FileInputStream(tempFile);
				fi.read(data);
				fi.close();
			} 
			catch (IOException e) 
			{
				throw new RuntimeException(e);
			}
			tempFile.delete(); tempFile = null;
		}
		else { assert(tempFile == null); }
		
		return data;
	}

	public synchronized File getFile() { return tempFile; }
}
