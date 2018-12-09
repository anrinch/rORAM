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
/** Unsafe ParallelRunner **/
public class InitRunner extends ParallelRunner 
{
	public InitRunner(ExternalClientInterface c, ExternalStorageInterface s)  
	{ super(c, s, false); }
}
