package runner;


import java.io.File;


import crypto.CryptoProvider;
import Interfaces.CompletionCallback;
import Interfaces.ExternalClientInterface;
import Interfaces.ExternalStorageInterface;
import services.Request;
import services.ScheduledRequest;
import services.RequestSource;
import utils.*;
/**
 * Runs requests serially (i.e., one at a time).
 */
public class SerialRunner extends AbstractRunner 
{
	public SerialRunner(ExternalClientInterface c, ExternalStorageInterface s) { super(c, s); }

	@Override
	public boolean onScheduled(ScheduledRequest sreq) 
	{
		sreq.waitUntilReady();		// force serialization (i.e., no parallelism)
		return sreq.wasSuccessful(); 
	}

	@Override
	public CompletionCallback onNew(Request req) { return null; } 
}
