package crypto;

import java.nio.ByteBuffer;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import utils.ClientParameters;
import data.Header;
import utils.EncodingUtils;
import utils.Errors;
/**
 * Provides cryptographic utility functions.
 * 
 * <p>
 * <p>
 * <h4>Implementation notes:</h4>
 * <ul>
 * <li>Uses the <a href="https://en.wikipedia.org/wiki/Singleton_pattern">Singleton</a> design pattern.</li>
 * </ul>
 * <p>
 */
public class CryptoProvider 
{
	private static final CryptoProvider instance = new CryptoProvider();
	private CryptoProvider() {}
	
	public static CryptoProvider getInstance() { return instance; }
	
	private ClientParameters clientParams = ClientParameters.getInstance();
	private EncodingUtils encodingUtils = EncodingUtils.getInstance();
	private SecureRandom rng = new SecureRandom();
	
	private byte[] getHashKeyStream(byte[] rBytes, byte[] encryptionKey, int contentByteSize)
	{
		final int rByteSize = rBytes.length;
		final int retByteSize = contentByteSize + rByteSize;
		byte[] ret = new byte[retByteSize];
		try 
		{
			final int intByteSize = 4;
			final int hashByteSize = 20;
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			//final int hashByteSize = 64;
			//MessageDigest md = MessageDigest.getInstance("SHA-512");
			//final int hashByteSize = 32;
			//MessageDigest md = MessageDigest.getInstance("SHA-256");
			
			int iter = (int) Math.ceil(contentByteSize / (double)hashByteSize);
			byte[] tmp = new byte[iter * hashByteSize];
		
			for (int i=0; i < iter; i++) 
			{
				ByteBuffer buf = ByteBuffer.allocate(intByteSize).putInt(i); byte[] iterBytes = buf.array();
				md.update(iterBytes);
				md.update(rBytes);
				md.update(encryptionKey);
				byte[] digest = md.digest(); assert(digest.length == hashByteSize);
				System.arraycopy(digest, 0, tmp, i*hashByteSize, hashByteSize);
			}			
			System.arraycopy(rBytes, 0, ret, 0, rByteSize); // set r
			System.arraycopy(tmp, 0, ret, rByteSize, contentByteSize); // set PRF output
		} 
		catch (Exception e) { Errors.error(e); }
		return ret;
	}
	private byte[] getCipherKeyStream(byte[] rBytes, byte[] encryptionKey, int contentByteSize)
	{
		/** See: http://stackoverflow.com/questions/3451670/java-aes-and-using-my-own-key
		 * and: http://stackoverflow.com/questions/16292694/java-simulating-a-stream-cipher-with-aes-ctr **/
		final int rByteSize = rBytes.length;
		final int retByteSize = contentByteSize + rByteSize;
		byte[] ret = new byte[retByteSize];
		
		try 
		{
			MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
			sha1.update(encryptionKey); //sha1.update(rBytes);
			byte[] key = sha1.digest();
			key = Arrays.copyOf(key, 16); // use only first 128 bit
		
			SecretKeySpec secretKeySpec = new SecretKeySpec(key, "AES");
			
			byte[] iv = sha1.digest();
			iv = Arrays.copyOf(iv, 16); // use only first 128 bit
			
			IvParameterSpec ivparam = new IvParameterSpec(iv);
			
			Cipher c = Cipher.getInstance("AES/CTR/NoPadding");
			c.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivparam);
			ByteBuffer in = ByteBuffer.allocate(contentByteSize);
			ByteBuffer out = ByteBuffer.wrap(ret); out.put(rBytes);
			//c.update(ret, 0, ret.length-rByteSize, ret, rByteSize);
			int bytesWritten = c.doFinal(in, out);
			
			assert(bytesWritten == contentByteSize);
		} 
		catch (Exception e) { Errors.error(e); }
		return ret;
	}
	
	private byte[] getKeyStream(byte[] rBytes, int contentByteSize) 
	{	
		int rByteSize = rBytes.length; 
		assert(rByteSize == clientParams.randomPrefixByteSize);
		assert(contentByteSize >= clientParams.minContentByteSize);
		
		byte[] encryptionKey = clientParams.encryptionKey;
		
		int retByteSize = contentByteSize + rByteSize;
		
		final boolean useHashKeyStream = false;
		//final boolean useHashKeyStream = true;
		
		if(useHashKeyStream == true) { return getHashKeyStream(rBytes, encryptionKey, contentByteSize); }
		else { return getCipherKeyStream(rBytes, encryptionKey, contentByteSize); }
	}
	
	public byte[] encryptBlock(Header header, byte[] payload)
	{
		assert(header != null && payload != null);

		byte[] data = encodingUtils.encodeBlock(header, payload); // encode the data

		return encrypt(data);
	}
	
	public byte[] encrypt(byte[] data)
	{		
		int rByteSize = clientParams.randomPrefixByteSize; 
		byte[] rBytes = new byte[rByteSize]; // randomness used to get IND-CPA
		rng.nextBytes(rBytes); 
		
		byte[] ks = getKeyStream(rBytes, data.length);
		assert((data.length + rByteSize) == ks.length);
		
		byte[] ret = new byte[rByteSize + data.length];
		System.arraycopy(data, 0, ret, rByteSize, data.length);
		
		assert(ret.length == ks.length);
		
		// xor the encoded data with the keystream (iterated PRF)
		for(int i=0; i < ret.length; i++) { ret[i] ^= ks[i]; }
		
		return ret;
	}
	
	public byte[] decrypt(byte[] indata) 
	{
		assert(indata != null);
		
		//byte[] data = new byte[indata.length];
		//System.arraycopy(indata, 0, data, 0, indata.length);
		
		// first recover r
		int rByteSize = clientParams.randomPrefixByteSize; 
		byte[] rBytes = new byte[rByteSize];
		//System.arraycopy(data, 0, rBytes, 0, rByteSize);
		System.arraycopy(indata, 0, rBytes, 0, rByteSize);
		
		//byte[] ret = new byte[data.length - rByteSize];
		byte[] ret = new byte[indata.length - rByteSize];
		
		byte[] ks = getKeyStream(rBytes, ret.length);
		//assert(data.length == ks.length);
		assert(indata.length == ks.length);
		
		// xor the encoded data with the keystream (iterated PRF)
		//for(int i=0; i < data.length; i++) { data[i] ^= ks[i]; }
		for(int i=rByteSize; i < ks.length; i++) { ret[i-rByteSize] = (byte)(indata[i] ^ ks[i]); }
		
		//System.arraycopy(data, rByteSize, ret, 0, ret.length);
		return ret;
	}

	public String getHexHash(byte[] data) 
	{
		MessageDigest sha1 = null;
		try { sha1 = MessageDigest.getInstance("SHA-1"); } catch (NoSuchAlgorithmException e) {	e.printStackTrace(); System.exit(-1); }
		return encodingUtils.toHexString(sha1.digest(data));
	}
	
	public String truncatedHexHashed(String input, int length)
	{
		assert(length > 0 && length <= 20);
		
		MessageDigest sha1 = null;
		try { sha1 = MessageDigest.getInstance("SHA-1"); } catch (NoSuchAlgorithmException e) {	e.printStackTrace(); System.exit(-1); }
		
		byte[] hash = sha1.digest(input.getBytes(Charset.forName("US-ASCII")));

		hash = Arrays.copyOf(hash, length);
		return DatatypeConverter.printHexBinary(hash);
	}
}