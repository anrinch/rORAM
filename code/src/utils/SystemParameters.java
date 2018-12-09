package utils;
import java.io.File;
public class SystemParameters {

private SystemParameters() {}
	
	private static final SystemParameters instance = new SystemParameters();
	public static SystemParameters getInstance() { return instance; }
	
	public String logsDirFP = "./log";

	public File credentials = new File("./credentials.file");
	
	public String localDirectoryFP = "/mnt/scratch";
	public String tempDirectoryFP = "./temp";
	
	public int storageOpMaxAttempts =4;
	public int clientReqMaxAttempts = 2;
}
