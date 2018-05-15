package com.china.dbtest.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.jmeter.protocol.tcp.sampler.AbstractTCPClient;
import org.apache.jmeter.protocol.tcp.sampler.ReadException;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.util.JOrphanUtils;
import org.apache.log.Logger;

public class StringLengthPrefixTcpClientImpl
  extends AbstractTCPClient
{
	private static final Logger log = LoggingManager.getLoggerForClass() ;
    private final int lengthPrefixLen = JMeterUtils.getPropDefault("tcp.stringlength.prefix.length", 6);
 
    public StringLengthPrefixTcpClientImpl() {
		super();
		if (log.isDebugEnabled()) {
			log.info("Created StringLengthPrefixTcpClientImpl");
	    }
		//log.info("Created StringLengthPrefixTcpClientImpl");
		// TODO Auto-generated constructor stub
	}


  public void write(OutputStream os, String s)
    throws IOException
  {
	  try {
          os.write(s.getBytes(Charset.forName("UTF-8")));
          os.flush();
      }
      catch (IOException e) {
          log.warn("Write error", e);
      }
  }
  
  public void write(OutputStream os, InputStream is)
    throws IOException
  {
	 throw new UnsupportedOperationException("NOT AVALIABLE");
  }
  
  public String read(InputStream is)
    throws ReadException
  {
		byte[] msg = new byte[0];
		byte[] headerByte = new byte[lengthPrefixLen];
		try {
			if (is.read(headerByte) != -1) {
				int length = Integer.parseInt(new String(headerByte));
				byte[] readBytes = new byte[length];
				byte[] totalBytes = new byte[length];
				int currentLength = 0;
				int readLength = 0;

				while (currentLength < length && (readLength = is.read(readBytes, 0, length)) != -1) {
					System.arraycopy(readBytes, 0, totalBytes, currentLength, readLength);
					currentLength += readLength;
				}
				return new String(totalBytes, "UTF-8");
			} else {
				throw new ReadException("CANNOT READ HEADER", new Exception(), JOrphanUtils.baToHexString(msg));
			}
		} catch (IOException e) {
			throw new ReadException("", e, JOrphanUtils.baToHexString(msg));
		}
  }

}
