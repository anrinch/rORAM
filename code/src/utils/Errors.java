package utils;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

public class Errors {
	private static Log log = Log.getInstance();
	private static boolean throwErrors = false;
	
	private static void _afterErrorAction(String msg, String lmsg)
	{
		if(throwErrors == true) { throw new RuntimeException(msg); }
		System.err.println(lmsg); System.err.flush();
		System.exit(-1);
	}
	
	public static void _error(String msg, String stackTrace, int c)
	{
		String lmsg = "Error: " + msg; 
		lmsg += stackTrace;

		log.append(lmsg, Log.ERROR); log.forceFlush();
		
		_afterErrorAction(msg, lmsg);
	}
	
	public static void error(String msg) { _error(msg, getStackTraceString(), 1); }
	
	private static String getStackTraceString(Throwable t) 
	{
		Writer w = new StringWriter();
	    PrintWriter printWriter = new PrintWriter(w);
	    t.printStackTrace(printWriter);
	    return w.toString();
	}
	
	private static String getStackTraceString() 
	{
		String ret = "";
		
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		for(int i=2; i<trace.length; i++) 
		{
			ret += "\tat " + trace[i].toString() + "\n";
		}
		
		return ret;
	}
	
	public static void error(Throwable e) { _error("", getStackTraceString(e), 1); }
	
	public static void warn(String msg) { log.append(msg, Log.WARNING); }

	public static void warn(Throwable e) { warn(e.toString()); }
	
	public static void _verify(boolean condition, String msgIfFail, int c)
	{
		final String msg = "Verify failed: " + msgIfFail;
		boolean assertsEnabled = Errors.class.desiredAssertionStatus();
		if(condition == false)
		{
			if(assertsEnabled == true) 
			{
				try	{ assert(condition) : msg; } catch(AssertionError e) { error(msg); }
			}
			else { _error(msg, null, c+1); }
		}
	}
	
	public static void verify(boolean condition) { _verify(condition, "", 1); }
	public static void verify(boolean condition, String msg) { _verify(condition, msg, 1); }
}

