import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;

import Construction.SchemeFactory;
import Interfaces.ExternalClientInterface;
import Interfaces.ExternalStorageInterface;

import crypto.CryptoProvider;
import data.Header;
import evaluation.PerformanceEvaluationLogger;

import runner.*;
import services.ExtractedTraceRequestSource;
import services.FileRequestSource;
import services.RandomRequestSource;
import services.RequestSource;
import test.ClientTester;
import utils.ClientParameters;
import utils.Errors;
import utils.Log;
import utils.SessionState;


public class CloudExperiments {
	private static Vector<String> getNonOptionalArgs(String[] args)
	{
		Vector<String> ret = new Vector<String>();
		for(int i=0; i<args.length; i++)
		{
			String arg = args[i];
			if(arg.startsWith("--") == false) { ret.add(arg); }
		}
		
		return ret;
	}
	
	private static boolean containsOptionalFlag(String[] args, String flag)
	{
		boolean ret = false;
		
		if(flag.startsWith("--") == false) { flag = "--" + flag; }
		
		for(int i=0; i<args.length; i++)
		{
			String arg = args[i];
			if(arg.contains("=") == true) { arg = arg.substring(0, arg.indexOf("=")); }
			
			if(arg.equalsIgnoreCase(flag) == true) { return true; }
		}
		
		return ret;
	}
	
	private static String getArgValue(String[] args, String key, String defVal)
	{
		key = "--" + key + "=";
		
		for(int i=0; i<args.length; i++)
		{
			String arg = args[i];
			if(arg.startsWith(key) == true) 
			{
				return arg.substring(key.length());
			}
		}
		
		if(defVal == null) { Errors.error("No value for arg: " + key + "!"); }
		
		return defVal;
	}
	
	private static int getArgIntValue(String[] args, String key, int defVal)
	{
		String retStr = getArgValue(args, key, new Integer(defVal).toString());
		
		return Integer.parseInt(retStr);
	}
	
	private static long getArgLongValue(String[] args, String key, long defVal)
	{
		String retStr = getArgValue(args, key, new Long(defVal).toString());
		
		return Long.parseLong(retStr);
	}
	
	private static String checkArgsAndGetCmd(String[] args, Vector<String> nonOptArgs, int minNonOptArgs)
	{
		if(nonOptArgs.size() < minNonOptArgs)
		{
			usage();
			System.exit(-1);
		}
		
		String invoke = "CloudExperiments.java";
		for(int i=0; i<args.length; i++) { invoke += " " + args[i]; }
		
		return invoke;
	}
	
	
	
	// the available command-line arguments and their default values
	//
	private static final String sessionFileArg = "session-file";
	private static final String defSessionFileArg = "./state/session.state";
	
	private static final String debugArg = "debug";
	
	private static final String testOnlyArg = "test";
	private static final String generateArg = "generate";
	private static final int defGenerateArg = 10000;
	private static final String initLengthArg = "init-length";
	private static final int defInitLengthArg = 1000;
	
	private static final String rsFileArg = "rs-file";
	private static final String defRSFileArg = "./input.rs";
	
	private static final String resumeArg = "resume";	
	private static final String cloneStorageArg = "clone-storage-to";
	
	
	private static final String runnerArg = "runner";
	
	private static final String runnerArgValInit = "init";
	private static final String runnerArgValSerial = "serial";
	private static final String runnerArgValParallel = "parallel";
	private static final String runnerArgValTrack = "track";
	private static final String defRunnerArg = runnerArgValParallel;
	
	private static final String fastInitArg = "fast-init";
	
	private static final String perfSummaryFPArg = "perf-summary-file";
	private static final String defPerfSummaryFPArg = "./summary.perf";
	
	private static final String extractedTraceArg = "extracted";
	private static final String timingArg = "timing";
	private static final String countLogicalArg = "count-logical";
	private static final String noSplitArg = "nosplit";
	
	
	private static void usage()
	{
		final String progname = "CloudExperiments";
		String newUsage = progname;
		
		final String newSession = "[--" + sessionFileArg + "=<filepath>|" + defSessionFileArg + "]";
		final String test = "[--" + testOnlyArg + "]";
		
		final String generate = "[--" + generateArg + "=<genReq>|" + defGenerateArg + "] [--" + initLengthArg + "=<num>|max(" + defInitLengthArg + ", genReq/2)]"; 
		final String rsFile = "--" + rsFileArg + "=<rsFile>|" + defRSFileArg + "";
		
		final String runner = "[--" + runnerArg + "=<runner>|" + defRunnerArg + "]"; 
		
		final String fastInit = "[--" + fastInitArg + "]";
		
		newUsage += " " + newSession + " " + test + " " + rsFile;
		newUsage += " " + generate + " " + runner + " " + fastInit + " ";
		newUsage += "<numReqsToRun> <client> <storage>";
		
		String resumeUsage = progname;
		
		final String resumeSession = "--" + resumeArg + " --" + newSession;
		
		resumeUsage += " " + resumeSession + " " + runner + " <numReqs>";
		
		System.out.println("Usage (new session): " + newUsage);
		System.out.println("Usage (resume session): " + resumeUsage);
	}

	
	public static void main(String[] args)
	{	
		Vector<String> nonOptArgs = getNonOptionalArgs(args);
		
		SchemeFactory sf = SchemeFactory.getInstance();
		
		// start parsing
		SessionState ss = SessionState.getInstance();
		String sessionFilePath = getArgValue(args, sessionFileArg, defSessionFileArg);
		File sessionFile = new File(sessionFilePath);
		
		RequestSource rs = null;
		RequestSource inputRS = null;
		
		// first determine whether this will be a new session
		boolean resume = containsOptionalFlag(args, resumeArg);
		
		String invoke = checkArgsAndGetCmd(args, nonOptArgs, (resume == true) ? 1 : 3); 
		System.out.println("Running: " + invoke + "\n");
		System.out.println("Things are fine");
		boolean generateRS = false;		
		if(resume == true) 
		{
			System.out.println("I shouldnt be here");
			ss.load(sessionFile);
			ss.setReset(false);
			
			sf.setHeader(ss.client); // set header
		}
		else // init new session
		{			
			System.out.println("I am here");
			ss.setReset(true);
			
			ss.debug = containsOptionalFlag(args, debugArg);
			
			ss.testOnly = containsOptionalFlag(args, testOnlyArg);
			
			ss.rsFilePath = getArgValue(args, rsFileArg, defRSFileArg);
			
			ss.extracted = containsOptionalFlag(args, extractedTraceArg);
			ss.timing = containsOptionalFlag(args, timingArg);
			ss.countLogical = containsOptionalFlag(args, countLogicalArg);
			
			generateRS = containsOptionalFlag(args, generateArg);
			
			ss.client = nonOptArgs.get(1);
			ss.storage = nonOptArgs.get(2);
			
			sf.setHeader(ss.client); // set header
			
			ss.fastInit = containsOptionalFlag(args, fastInitArg);
			
			ss.nextReqId = 1;
			
			initClientParams(args);
		}
		
		// --- can only use log from this point
		// init log
		Log log = Log.getInstance();
		log.setDebug(ss.debug);
		log.initLogFiles(!ss.shouldReset());
		
		if(ss.shouldReset() == true) { Log.getInstance().append("Initializing new session (from " + sessionFilePath + "), command: \n\t\"" + invoke + "\"", Log.INFO); }
		else { log.append("Resuming session (from " + sessionFilePath + "), command: \n\t\"" + invoke + "\"", Log.INFO);	}
		
		String numReqsStr = nonOptArgs.get(0);
		int length = Integer.parseInt(numReqsStr);
		
		final String client = ss.client;
		final String storage = ss.storage;
		
		final String experimentDesc = getExperimentDesc();
	
		final String expDescStr = "Experiment desc: " + experimentDesc;
		System.out.println(expDescStr);
		log.append(expDescStr, Log.INFO);
		
		CryptoProvider cp = CryptoProvider.getInstance();
		String experimentHash = cp.getHexHash(experimentDesc.getBytes(Charset.forName("UTF-8"))).substring(0, 10);
		experimentHash = experimentHash.toLowerCase();
		ss.experimentHash = experimentHash;
		
		String storageKey = "CEP--" + experimentHash; storageKey = storageKey.toLowerCase();
		ss.storageKey = storageKey;
			
		String lmsg = "Storage key: " + storageKey + " (experiment hash: " + experimentHash + ")";
		System.out.println(lmsg); System.out.println();
		log.append(lmsg, Log.INFO);

		final String schemeStr = experimentHash;
		final File stateFile = new File("./state/" + experimentHash + ".state");
		
		if(generateRS == true)
		{
			int generateLength = getArgIntValue(args, generateArg, defGenerateArg);
			
			int initLength = getArgIntValue(args, initLengthArg, defInitLengthArg);
			
			if(containsOptionalFlag(args, initLengthArg) == false) { initLength = Math.max(generateLength/2, initLength); }
			if(initLength > generateLength) { usage(); System.exit(-1); }
			
			log.append("Generating random request sequence of length " + generateLength + " (init: " + initLength + ")...", Log.INFO);
			
			rs = new RandomRequestSource(initLength, generateLength);
			rs.dumpToFile(new File(ss.rsFilePath));
			
			log.append("Generated request sequence dumped to " + ss.rsFilePath, Log.INFO);
		}
		
		if(rs == null) 
		{ 
			if(ss.extracted == true)
			{
				log.append("Reading extracted trace from " + ss.rsFilePath, Log.INFO);
				
				ClientParameters clientParams = ClientParameters.getInstance();
				ExtractedTraceRequestSource ers = new ExtractedTraceRequestSource(new File(ss.rsFilePath), !clientParams.noSplit); 
				rs = ers;
				
				inputRS = ers.getInputRS();
			}
			else
			{
				log.append("Reading requests from " + ss.rsFilePath, Log.INFO);
				rs = new FileRequestSource(new File(ss.rsFilePath)); 
			}
		}
		Errors.verify(rs != null);
		
		rs.seek(ss.nextReqId); // seek the proper next request
		long startingReqId = rs.getNextRequestId();
		ss.nextReqId = (int)startingReqId;
		
		System.out.println("Starting from req " + startingReqId + " we will attempt to process " + length + " requests.");
		
		int reqsProcessed = 0;
		try
		{			
			boolean success = true;
			long t1 = System.currentTimeMillis();
			long t2 = 0;
			
			if(ss.testOnly == true)
			{
				if(containsOptionalFlag(args, runnerArg) == true)
				{
					String msg = "Runner parameter ignored in test only mode!";
					Errors.warn(msg); System.err.println("Warning: " + msg.toLowerCase());
				}
				
				System.out.println("Starting test...");
				
				assert(rs != null);
				int procCount = testScheme(client, storage, rs, length, ss);
				reqsProcessed += procCount;
				if(procCount >= length) { success = true; }	else { success = false; }
				
				t2 = System.currentTimeMillis();
			}
			else
			{				
				String storageCloneTo = null;
				if(containsOptionalFlag(args, cloneStorageArg) == true)
				{
					storageCloneTo = getArgValue(args, cloneStorageArg, null);
					storageCloneTo = ss.storageKey + "---" + storageCloneTo;
				}
				
				ExternalStorageInterface esi = sf.createStorage(storage, ss.shouldReset());
				ExternalClientInterface eci = sf.createClient(client);				
				
				String desc = "(client name: " + eci.getName() + ", synchronous: " + eci.isSynchronous() + ")";
				
				boolean unsafeRunner = (eci.getName().contains("ObliviStore") || eci.getName().contains("Proposed") || eci.getName().contains("FrameworkC") || eci.getName().contains("FC"));
				
				AbstractRunner runner = null;
				
				String runnerStr = getArgValue(args, runnerArg, defRunnerArg);
				switch(runnerStr)
				{
				case runnerArgValInit: runner = new InitRunner(eci, esi); break;
				case runnerArgValSerial: runner = new SerialRunner(eci, esi); break;
				case runnerArgValParallel: runner = new ParallelRunner(eci, esi, !unsafeRunner); break;
				case runnerArgValTrack: runner = new TrackRequestsRunner(eci, esi, !unsafeRunner, ss.timing, ss.countLogical); break;
				default:
					Errors.error("Unknown runner: " + runnerStr + "!");
				}
				
				System.out.println("Starting experiments [runner: " + runnerStr + "] " + desc);
				
				log.append("Starting experiments [runner: " + runnerStr + "] " + desc + "; will process " + length + " next requests.", Log.INFO);
				
				runner.open(stateFile, inputRS); // open
				
				reqsProcessed = runner.run(rs, length); // run
				// note: runner will increment ss.nextReqId
				
				log.append(reqsProcessed + " requests processed.", Log.INFO);
				
				runner.close(storageCloneTo);
				
				t2 = System.currentTimeMillis();

				
				final File logFile = new File("./log/" + schemeStr + ".perfeval.log");
				
				log.append("All done, dumping performance log to " + logFile.getPath() + "...", Log.INFO);
				
				
				PerformanceEvaluationLogger pelog = PerformanceEvaluationLogger.getInstance();
				pelog.dumpToFile(logFile);
				
				String perfSummaryFP = getArgValue(args, perfSummaryFPArg, defPerfSummaryFPArg);
				final File perfSummaryFile = new File(perfSummaryFP);
				pelog.summarizeToFile(perfSummaryFile, runnerStr);
				
				if(runnerStr.equals(runnerArgValTrack) == true)
				{
					File trackPELogFile = new File("./log/" + schemeStr + ".trackpe.log");
					printTrackPerf(trackPELogFile, (TrackRequestsRunner)runner);
				}
			}
			System.out.println("");
			
			double elapsedSecs = (t2 - t1)/1000.0;
			if(success == true)
			{
				System.out.println("Done -- " + reqsProcessed + " requests processed in " + elapsedSecs + " seconds!");
			}
			else
			{
				System.out.println("Failed (in " + elapsedSecs + " seconds)!");
			}
		}
		catch(Exception | Error e) { Errors.error(e); }
		
		ss.save(sessionFile);
		
		log.append("Exiting.", Log.INFO);
		log.forceFlush(); 
	}

	private static void printTrackPerf(File trackPELogFile, TrackRequestsRunner runner) 
	{
		Errors.verify(runner != null);
		List<Entry<String, List<Number>>> list = runner.getStats();
		
		SessionState ss = SessionState.getInstance();
		ClientParameters clientParams = ClientParameters.getInstance();
		
		boolean fileExists = trackPELogFile.exists();
		
		try
		{
			FileWriter fw = new FileWriter(trackPELogFile, true);
			BufferedWriter bw = new BufferedWriter(fw);
			
			String line = "";
			if(fileExists == false)
			{
				line += ss.experimentHash + ", " + ss.client + ", " + ss.storage + ", ";			
				line += clientParams.maxBlocks + ", " + clientParams.contentByteSize;
			}
			else { line += "------------------------------------------------------------------------"; }
			
			bw.write(line); bw.newLine();
			
			for(Entry<String, List<Number>> entry : list)
			{
				String name = entry.getKey();
				List<Number> l = entry.getValue();
				Errors.verify(l.size() == 8);
				
				long start = (Long)l.get(0);
				long end = (Long)l.get(1);
				long elapsed = end - start;
				long ltt = (Long)l.get(2);
				long stt = (Long)l.get(3);
				long rtt = (Long)l.get(4);
				
				int byteSize = (Integer)l.get(5);
				int blocks = (Integer)l.get(6);
				
				double ratio = (Double)l.get(7);
				
				String str = name + ", " + byteSize + ", " + blocks + ", " + String.format("%.3f", ratio);
				str += ", " + ltt + ", " + stt + ", " + rtt + ", " + start + ", " + end + ", " + elapsed;
				bw.write(str); bw.newLine();
			}
			 
			bw.flush();
			bw.close();
		}
		catch(Exception e) { Errors.error(e); }
	}

	private static int testScheme(String client, String storage, RequestSource rs, int length, SessionState ss) 
	{
		SchemeFactory sf = SchemeFactory.getInstance();
		ExternalClientInterface eci = sf.createClient(client);
		
		ExternalStorageInterface esi = sf.createStorage(storage, ss.shouldReset());
		
		ClientTester tester = new ClientTester(eci, esi);
		
		int reqsProcessed = tester.runTest(rs, length, ss);
		boolean success = !tester.hasFailed();
		
		if(success == true)
		{
			System.out.println("Successfully tested client: '" + client + "'!");
		}
		else { System.out.println("Test for client: '" + client + "' failed!"); }
		
		return reqsProcessed;
	}
	
	private static String maxBlocksArg = "max-blocks";
	private static long defMaxBlocks = 1 << 18;
	
	private static String blockByteSizeArg = "block-byte-size";
	private static int defBlockByteSize = 4 * 1024;
	
	private static String localPosMapCutoffArg = "posmap-cutoff";
	private static int defLocalPosMapCutoff = 1;
	
	private static void initClientParams(String[] args)
	{				
		ClientParameters clientParams = ClientParameters.getInstance();
		int encryptionOverhead = Header.getByteSize() + clientParams.randomPrefixByteSize;
		
		clientParams.maxBlocks = getArgLongValue(args, maxBlocksArg, defMaxBlocks);
		clientParams.contentByteSize = getArgIntValue(args, blockByteSizeArg, defBlockByteSize) - encryptionOverhead;
		Errors.verify(clientParams.contentByteSize > 0);
		
		clientParams.localPosMapCutoff = getArgIntValue(args, localPosMapCutoffArg, defLocalPosMapCutoff);
		
		SecureRandom rng = new SecureRandom();
		clientParams.encryptionKey = new byte[clientParams.encryptionKeyByteSize];
		rng.nextBytes(clientParams.encryptionKey);
		
		clientParams.noSplit = containsOptionalFlag(args, noSplitArg);
	}
	
	private static String getExperimentDesc()
	{
		String ret = "";
		
		SessionState ss = SessionState.getInstance();
		ClientParameters clientParams = ClientParameters.getInstance();
		
		String schemeDesc = "[Scheme] " + ss.client + " with " + ss.storage + " storage";
		if(ss.fastInit == true) { schemeDesc += " (fast init)";}
		ret += schemeDesc + "\n\t";
		
		String inputDesc = "[Input] rsFile: " + ss.rsFilePath + " (test: " + ss.testOnly + ")";
		ret += inputDesc + "\n\t";
		
		String encryptionDesc = "[Encryption] key hash: " + CryptoProvider.getInstance().getHexHash(clientParams.encryptionKey);
		encryptionDesc += ", random prefix size: " + clientParams.randomPrefixByteSize;
		encryptionDesc += ", header size: " + Header.getByteSize();
		ret += encryptionDesc + "\n\t";
		
		String ramDesc = "[RAM] N: " + clientParams.maxBlocks + ", l: " + clientParams.contentByteSize;
		ramDesc += ", local posmap cutoff: " + clientParams.localPosMapCutoff;
		ret += ramDesc + "\n\t";
		
		String hostname = "";
		try { hostname = InetAddress.getLocalHost().getHostName(); } 
		catch (UnknownHostException e) { Errors.error(e); }
		String machineDesc = "[Machine] hostname: " + hostname;
		System.out.println(machineDesc);
		//ret += machineDesc + "\n";
		
		return ret;
	}
}
