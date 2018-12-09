package services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import services.Request;

/**
 * Implements a request source backed by a file.
 * Each row of the file contains the description of a single request.
 *
 */
public class FileRequestSource extends RequestSource 
{
	public FileRequestSource(File inputFile)
	{
		try
		{
			FileReader fr = new FileReader(inputFile);
			BufferedReader br = new BufferedReader(fr);
			
			String line = "";
			while((line = br.readLine()) != null)
			{
				Request req = Request.fromString(line);
				requests.put(req.getId(), req);
			}
			
			br.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
		
		rewind();
	}
}
