package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

public class Log {
protected SystemParameters sysParams = SystemParameters.getInstance();
	
	private static Log instance = null;	
	private Log() { setDebug(false); } // by default -> false
	
	public static synchronized Log getInstance() 
	{ 
		if(instance == null) { instance = new Log(); }
		return instance;
	}
	
	public static final int ERROR = 0x100; 
	public static final int WARNING = 0x040; 
	public static final int DEBUG = 0x010;  
	public static final int INFO = 0x004;
	public static final int TRACE = 0x001; 
	
	protected Logger mainLogger = null;
	protected Logger allLogger = null;
	
	protected int mainLevel = TRACE;
	
	protected boolean flushAfterAppend = false; // false unless debug is on
	
	protected class Logger
	{
		public static final int bufferCapacity = 50;
		protected BufferedWriter bw = null;
		protected Vector<String> buffer = new Vector<String>();
		
		protected Logger(File logFile, boolean append)
		{
			try
			{
				FileWriter fw = new FileWriter(logFile, append);
				bw = new BufferedWriter(fw);
			}
			catch(IOException e) { Errors.error(e); }
		}
		
		protected void append(String entry)
		{
			buffer.add(entry);
			if(buffer.size() >= bufferCapacity)	{ flush(); }
		}

		protected void flush() 
		{			
			try
			{
				for(String entry : buffer) { bw.write(entry); bw.newLine(); }
				bw.flush();
				buffer.clear();
			}
			catch(IOException e) { Errors.error(e); }
		}
	}
	
	protected String getCurrentTimeStamp() 
	{
	    //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
	    return sdf.format(new Date());
	}
	
	public void setDebug(boolean debug)
	{
		flushAfterAppend = debug; 
		setMainLevel((debug == true) ? TRACE : INFO);		
	}
	
	public void initLogFiles(boolean append)
	{
		String mainLoggerFP = sysParams.logsDirFP + "/main.log";
		mainLogger = new Logger(new File(mainLoggerFP), append);
		
		String allLoggerFP = sysParams.logsDirFP + "/all.log";
		allLogger = new Logger(new File(allLoggerFP), append);
		
		forceFlush();
	}
	
	public void setMainLevel(int level) { mainLevel = level; }
	
	public synchronized void append(String msg, int level)
	{
		String dateTime = getCurrentTimeStamp();
		
		String levelStr = null;
		switch(level)
		{
		case ERROR: levelStr = "E";/*"ERROR";*/ break;
		case WARNING: levelStr = "W";/*"WARNING";*/ break;
		case DEBUG: levelStr = "D"; /*"DEBUG";*/ break;
		case INFO: levelStr = "I"; /*"STATUS";*/ break;
		case TRACE: levelStr = "T"; /*"INFO";*/ break;
		default:
			Errors.error("Invalid log level: " + level + " !");
		}
		
		String threadDesc = "T" + String.format("%02d", Thread.currentThread().getId());
		
		String entry = "[<" + levelStr + "> - " + threadDesc + " - " + dateTime + "] " + msg;
		
		if(level >= mainLevel) { mainLogger.append(entry); }
		allLogger.append(entry);
		
		if(flushAfterAppend == true || (level >= WARNING)) { forceFlush(); }
	}
	
	public synchronized void forceFlush() 
	{
		mainLogger.flush();
		allLogger.flush();
	}
	
	public boolean shouldLog(int level) { return (level >= mainLevel); }
}
