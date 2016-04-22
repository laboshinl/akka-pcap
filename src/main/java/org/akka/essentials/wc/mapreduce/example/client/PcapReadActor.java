package org.akka.essentials.wc.mapreduce.example.client;

import java.io.*;
import java.nio.ByteBuffer;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;
import akka.event.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.math.*;

public class PcapReadActor extends UntypedActor {
	final LoggingAdapter logger = Logging
			.getLogger(getContext().system(), this);

	public static int byteArrayToLeInt(byte[] encodedValue) {
		int value = (encodedValue[3] << (Byte.SIZE * 3));
		value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 2);
		value |= (encodedValue[1] & 0xFF) << (Byte.SIZE * 1);
		value |= (encodedValue[0] & 0xFF);
		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof String) {
			String fileName = (String) message;
			try {
				byte[] packet = null;
				int numberOfTasks = 0;

				RandomAccessFile f = new RandomAccessFile(Thread
						.currentThread().getContextClassLoader()
						.getResource(fileName).getPath(), "r");
				int bytes_skip = 8;
				while (bytes_skip < (int) f.length()) {
					f.seek(bytes_skip);
					byte[] captured_size = new byte[4];
					f.read(captured_size);
					f.seek(bytes_skip + 4);
					byte[] untruncated_size = new byte[4];
					f.read(untruncated_size);
					int captured = byteArrayToLeInt(captured_size);
					int untruncated = byteArrayToLeInt(untruncated_size);
					if (captured == untruncated && untruncated < 65536 && untruncated > 41) {
						f.seek(bytes_skip + 20);
						byte[] type = new byte[2];
						f.read(type);
						//logger.info("0x800 " + Integer.parseInt("800", 16));// Hex.encodeHexString(type));
						Short ethertype = ByteBuffer.wrap(type).getShort();
						if (new IntRange(Integer.parseInt("0", 16), Integer.parseInt("5dc", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("600", 16), Integer.parseInt("661", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("800", 16), Integer.parseInt("808", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("884", 16), Integer.parseInt("89a", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("b00", 16), Integer.parseInt("b07", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("bad", 16), Integer.parseInt("baf", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("1000", 16), Integer.parseInt("10ff", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("2000", 16), Integer.parseInt("207f", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("22e0", 16), Integer.parseInt("22f2", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("86dd", 16), Integer.parseInt("8fff", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("9000", 16), Integer.parseInt("9003", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("9040", 16), Integer.parseInt("905f", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("c020", 16), Integer.parseInt("c02f", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("c220", 16), Integer.parseInt("c22f", 16)).containsInteger(ethertype) ||
								new IntRange(Integer.parseInt("fea0", 16), Integer.parseInt("feaf", 16)).containsInteger(ethertype) || 
								new IntRange(Integer.parseInt("ff00", 16), Integer.parseInt("ff0f", 16)).containsInteger(ethertype) ||
								Integer.parseInt("81c", 16) == ethertype ||
								Integer.parseInt("844", 16) == ethertype ||
								Integer.parseInt("900", 16) == ethertype ||
								Integer.parseInt("a00", 16) == ethertype ||
								Integer.parseInt("a01", 16) == ethertype ||
								Integer.parseInt("1600", 16) == ethertype ||
								Integer.parseInt("22df", 16) == ethertype ||
								Integer.parseInt("9999", 16) == ethertype ||
								Integer.parseInt("9c40", 16) == ethertype ||
								Integer.parseInt("a580", 16) == ethertype ||
								Integer.parseInt("fc0f", 16) == ethertype ||
								Integer.parseInt("ffff", 16) == ethertype) {
							//logger.info("Ethertype " + Hex.encodeHexString(type));
							f.seek(bytes_skip + 8); //-8
							packet = new byte[untruncated]; //+16
							f.read(packet);
							
							//logger.info("data " + Hex.encodeHexString( packet ) );
							getSender().tell(packet, getSelf());
							numberOfTasks++;
							bytes_skip += (untruncated + 15);

						}
						else{
						bytes_skip += 1;
						}

					} else {
						bytes_skip += 1;
					}
				}
				f.close();
				logger.info("All packets send !");
				// byte[] b = new byte[(int)f.length()];
				// f.read(b);
				// f.close();

//				BufferedReader reader = new BufferedReader(
//						new InputStreamReader(Thread.currentThread()
//								.getContextClassLoader().getResource(fileName)
//								.openStream()));
//				String line = null;
//				numberOfTasks = 0;
//				while ((line = reader.readLine()) != null) {
//					getSender().tell(line, getSelf());
//					numberOfTasks++;
//				}
//				logger.info("All lines send !");

				getSender().tell(new TaskInfo(numberOfTasks), getSelf());
			} catch (IOException x) {
				logger.error("IOException: %s%n", x);
			}
		} else
			throw new IllegalArgumentException("Unknown message [" + message
					+ "]");
	}
}