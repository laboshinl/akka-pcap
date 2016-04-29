package ru.laboshinl.pcap.akka.client;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import akka.actor.*;
import akka.event.*;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.math.*;

import ru.laboshinl.pcap.akka.common.*;
import static org.bitbucket.dollar.Dollar.*;

public class PcapReadActor extends UntypedActor {
	final LoggingAdapter logger = Logging
			.getLogger(getContext().system(), this);

	
	final List<Integer> validEthertypes = 
			$(Integer.parseInt("800", 16), Integer.parseInt("808", 16))
			.concat($(Integer.parseInt("0", 16), Integer.parseInt("5dc", 16)))
			.concat($(Integer.parseInt("884", 16), Integer.parseInt("89a", 16)))
			.concat($(Integer.parseInt("884", 16), Integer.parseInt("89a", 16)))
			.concat($(Integer.parseInt("b00", 16), Integer.parseInt("b07", 16)))
			.concat($(Integer.parseInt("bad", 16), Integer.parseInt("baf", 16)))
			.concat($(Integer.parseInt("1000", 16), Integer.parseInt("10ff", 16)))
			.concat($(Integer.parseInt("2000", 16), Integer.parseInt("207f", 16)))
			.concat($(Integer.parseInt("22e0", 16), Integer.parseInt("22f2", 16)))
			.concat($(Integer.parseInt("86dd", 16), Integer.parseInt("8fff", 16)))
			.concat($(Integer.parseInt("9000", 16), Integer.parseInt("9003", 16)))
			.concat($(Integer.parseInt("9040", 16), Integer.parseInt("905f", 16)))
			.concat($(Integer.parseInt("c020", 16), Integer.parseInt("c02f", 16)))
			.concat($(Integer.parseInt("c220", 16), Integer.parseInt("c22f", 16)))
			.concat($(Integer.parseInt("fea0", 16), Integer.parseInt("feaf", 16)))
			.concat($(Integer.parseInt("ff00", 16), Integer.parseInt("ff0f", 16)))
			.concat(Integer.parseInt("81c", 16)).concat(Integer.parseInt("844", 16))
			.concat(Integer.parseInt("900", 16)).concat(Integer.parseInt("a00", 16))
			.concat(Integer.parseInt("a01", 16)).concat(Integer.parseInt("22df", 16))
			.concat(Integer.parseInt("9999", 16)).concat(Integer.parseInt("9c40", 16))
			.concat(Integer.parseInt("a580", 16)).concat(Integer.parseInt("fc0f", 16))
			.concat(Integer.parseInt("ffff", 16)).sort().toList();

	
	public static int byteArrayToLeInt(byte[] encodedValue) {
		int value = (encodedValue[3] << (Byte.SIZE * 3));
		value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 2);
		value |= (encodedValue[1] & 0xFF) << (Byte.SIZE * 1);
		value |= (encodedValue[0] & 0xFF);
		return value;
	}
	
	public static int byteArrayToLeShort(byte[] encodedValue) {
		int value = ((encodedValue[0]& 0xFF) << (Byte.SIZE * 1));
		value |= (encodedValue[1] & 0xFF);
		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof String) {
			String fileName = (String) message;
		    //System.out.println(validEthertypes);
			try {
				byte[] packet = null;
				int numberOfTasks = 0;

				RandomAccessFile f = new RandomAccessFile(Thread
						.currentThread().getContextClassLoader()
						.getResource(fileName).getPath(), "r");
//				FileSystem fs = FileSystem.Factory.get();
//				AlluxioURI path = new AlluxioURI(fileName);
				// Open the file for reading and obtains a lock preventing deletion
//				FileInStream f = fs.openFile(path);
				// Read data
//				byte [] data = new byte[1024];
//				in.read(data);
//				System.out.println(data);
//				// Close file relinquishing the lock
//				in.close();
				int bytesSkip = 0;
				int skipCounter = 0;
				int counter = 0;
				int prevCounter = -1;
				int failedPackets = 0;
				//List<byte[]> packets = new ArrayList<byte[]>();
				while (/*f.remaining()>0*/ bytesSkip < (int) f.length()) {
					f.seek(bytesSkip);
					byte[] captured_size = new byte[4];
					f.read(captured_size);
					f.seek(bytesSkip + 4);
					byte[] untruncated_size = new byte[4];
					f.read(untruncated_size);

					int captured = byteArrayToLeInt(captured_size);
					int untruncated = byteArrayToLeInt(untruncated_size);
					if (captured == untruncated && untruncated < 65536 && untruncated > 41) {
						//logger.info("one {} two {} skip {}", Hex.encodeHexString(untruncated_size),  Hex.encodeHexString(captured_size), bytes_skip);
						f.seek(bytesSkip + 20);
						byte[] type = new byte[2];
						f.read(type);
						//logger.info("0x800 " + Integer.parseInt("800", 16));// Hex.encodeHexString(type));
						int ethertype = byteArrayToLeShort(type);
						if (validEthertypes.contains(ethertype)){
							if (skipCounter > 0){
								logger.error("Skipped {} bytes {}", skipCounter, fileName);
								skipCounter = 0;
							}
							//logger.info("Ethertype " + Hex.encodeHexString(type));
							f.seek(bytesSkip + 8); //-8
					        if( f.length() < bytesSkip + 8 + untruncated){
					        	logger.error("Need extra {} bytes {}", bytesSkip + 8 + untruncated - f.length(), fileName);
					        	continue;
					        }
							packet = new byte[untruncated]; //+16
							f.read(packet);
							//packets.add(packet);
							counter ++;
							bytesSkip += (untruncated + 16);

							//logger.info("data " + Hex.encodeHexString( packet ) );
							//if(packets.size() == 100 || bytes_skip >= (int) f.length() ){
							if (ethertype == Integer.parseInt("800", 16)) {
								getSender().tell(packet, getSelf());
								numberOfTasks++;
							}
							//packets.clear();
							//}


						}
						else{
							//logger.error("Wrong EtherType " + Hex.encodeHexString(type) + " after packet " + counter + " at {} {} {}" , bytesSkip, Integer.parseInt("86dd", 16), byteArrayToLeShort(type)  ) ;
						    bytesSkip += 1;
						    skipCounter ++;
						}

					} else {
						bytesSkip += 1;
						skipCounter ++;
//						if(counter != prevCounter){
//						logger.error("Seeking after {}. {} Packets Failed", counter, failedPackets);
//						prevCounter = counter;
//						failedPackets ++;
						//}
					}
				}
				f.close();
				logger.error("{} packets send !", counter);

				getSender().tell(new TaskInfo(numberOfTasks), getSelf());
			} catch (IOException x) {
				logger.error("IOException: %s%n", x);
			}
		} else
			throw new IllegalArgumentException("Unknown message [" + message + "]");
	}
}