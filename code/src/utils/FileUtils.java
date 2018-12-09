package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {
private static final FileUtils instance = new FileUtils();
	
	private FileUtils() {}
	
	public static FileUtils getInstance() { return instance; }
	
	public void initializeDirectory(String dirFP, boolean reset)
	{
		File dir = new File(dirFP);
		if(dir.exists() == true) 
		{ 
			if(reset == true)
			{
				for(File f : dir.listFiles()) { f.delete(); }
				dir.delete(); 
			}
		}
		dir.mkdir();
	}
	
	public void flatDirectoryCopy(String srcFP, String dstFP)
	{
		File dstDir = new File(dstFP);
		{ // init make sure destination dir
			
			if(dstDir.exists() == true) 
			{ 
				for(File f : dstDir.listFiles()) { f.delete(); }
				dstDir.delete(); 
			}
			dstDir.mkdir();
		}
		
		File srcDir = new File(srcFP);
		
		if(srcDir.exists() == false) { Errors.error("Coding FAIL!"); }
		
		File[] files = srcDir.listFiles();
		
		Path dstPath = Paths.get(dstDir.getAbsolutePath());
		
		for(File f : files)
		{
			Path dstFilePath = dstPath.resolve(f.getName());
			
			try { Files.copy(Paths.get(f.getAbsolutePath()), dstFilePath); } 
			catch (IOException e) { throw new RuntimeException(e); }
		}		
	}

	public long flatDirectoryByteSize(String dirFP) 
	{
		long ret = 0;
		File dir = new File(dirFP);
		
		if(dir.exists() == false) { Errors.error("Coding FAIL!"); }
		
		File[] files = dir.listFiles();
		for(File f : files)
		{
			assert(f.isDirectory() == false);
			ret += f.length();
		}
		
		return ret;
	}
	
	public List<String> listFilenames(String dirFP) 
	{
		List<String> ret = new ArrayList<String>();
		File dir = new File(dirFP);
		
		if(dir.exists() == false) { Errors.error("Coding FAIL!"); }
		
		File[] files = dir.listFiles();
		for(File f : files)
		{
			assert(f.isDirectory() == false);
			ret.add(f.getName());
		}
		
		return ret;
	}
	
	
	/*** code from http://stackoverflow.com/questions/106770/standard-concise-way-to-copy-a-file-in-java ***/
	public boolean copy(File sourceFile, File destFile)
	{
		try 
		{
		    if(!destFile.exists()) { destFile.createNewFile(); }
	
		    FileChannel source = null; FileChannel destination = null;
	
		    try 
		    {
		        source = new FileInputStream(sourceFile).getChannel();
		        destination = new FileOutputStream(destFile).getChannel();
		        destination.transferFrom(source, 0, source.size());
		    }
		    finally 
		    {
		        if(source != null) { source.close(); }
		        if(destination != null) { destination.close(); }
		    }
		}
		catch(IOException e) { return false; }
		return true;
	}
}
