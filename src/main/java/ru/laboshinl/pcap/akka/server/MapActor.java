package ru.laboshinl.pcap.akka.server;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.Hex;

import ru.laboshinl.pcap.akka.client.PcapReadActor;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.*;

public class MapActor extends UntypedActor {
	final LoggingAdapter logger = Logging
			.getLogger(getContext().system(), this);

	private ActorRef actor = null;

	public MapActor(ActorRef inReduceActor) {
		actor = inReduceActor;
	}

	private String ipSrc(byte[] packet) {
		String src_address = "";
		for (int i = 26; i < 30; i++) {
			if (i != 26)
				src_address += ".";
			// else
			// src_address += "ip.src==";
			src_address += (int) (packet[i] & 0xFF);
		}
		return src_address;
	}

	private String ethType(byte[] packet) {
		byte[] type = new byte[2];
		type[0] = packet[12];
		type[1] = packet[13];
		return Hex.encodeHexString(type);
	}

	private int ipProto(byte[] packet) {
		return packet[23] & 0xFF;
	}

	private int tcpSrcPort(byte[] packet) {
		int value = (packet[34] & 0xFF) << (Byte.SIZE * 1);
		value |= (packet[35] & 0xFF);
		return value;
		// return Hex.encodeHexString(type);
	}

	private int tcpDstPort(byte[] packet) {
		int value = (packet[36] & 0xFF) << (Byte.SIZE * 1);
		value |= (packet[37] & 0xFF);
		return value;
	}

	private int tcpPush(byte[] packet) {
		// byte[] type = new byte[2];
		// type[0]=packet[46];
		// type[1]=packet[47];
		// return Hex.encodeHexString(type);
		// int value = (packet[47] & 0xFF) << (Byte.SIZE * 1);
//		logger.info("Flags" + Integer.toBinaryString(packet[47]));
		int value = 0;
        if(packet.length > 46){
		 value = (packet[47] >> 3) & 1;
        }

//		logger.info("Flags7" + ((packet[47] >> 7) & 1));
//		logger.info("Flags6" + ((packet[47] >> 6) & 1));
//		logger.info("Flags5" + ((packet[47] >> 5) & 1));
//		logger.info("Flags4" + ((packet[47] >> 4) & 1));
//		logger.info("Flags3" + ((packet[47] >> 3) & 1));
//		logger.info("Flags2" + ((packet[47] >> 2) & 1));
//		logger.info("Flags1" + ((packet[47] >> 1) & 1));
//		logger.info("Flags0" + ((packet[47] >> 0) & 1));
		return value;
	}
	
	private int tcpDataSize(byte[] packet) {
		
		//int ipHeaderLen = (packet[14] & 0xFF);
		int totalLen = (packet[16] & 0xFF) << (Byte.SIZE * 1);
		totalLen |= (packet[17] & 0xFF);
		//int tcpHeaderLen = (packet[46] & 0xFF);
		//		logger.info("Flags7" + ((packet[47] >> 7) & 1));
        logger.info("Total =" + (totalLen -52));
        return totalLen - 52;
		//return (totalLen - tcpHeaderLen - ipHeaderLen)/8;
	}

	private String ipDst(byte[] packet) {
		String dst_address = "";
		for (int i = 30; i < 34; i++) {
			if (i != 30)
				dst_address += ".";
			// else
			// dst_address += "ip.dst==";
			dst_address += (int) (packet[i] & 0xFF);
		}
		return dst_address;
	}

	private List<Result> evaluateExpression(byte[] packet) {
		List<Result> list = new ArrayList<Result>();
		//tcpPush(packet);
		if (ipProto(packet) == 6  && tcpDataSize(packet) > 20 /* &&  tcpPush(packet) == 1 */)  {
//			Result result = new Result(Long.toString((long) (ipDst(packet)
//					+ ":" + Integer.toString(tcpDstPort(packet))).hashCode()
//					+ (long) (ipSrc(packet) + ":" + Integer
//							.toString(tcpSrcPort(packet))).hashCode()), 1);

			Result result = new Result(ipSrc(packet)+ ":" + tcpSrcPort(packet) + "->" + ipDst(packet) + ":" + tcpDstPort(packet), 1);
			result.setData(Arrays.copyOfRange(packet, 54, packet.length));
			result.setSequence(seqNumber(packet));
			//logger.info("Sequence =" + seqNumber(packet));
			list.add(result);
			//logger.info("Flags " + tcpPush(packet));
		}

		return list;
	}

	private long seqNumber(byte[] packet){
		long value = 
		        ((packet[41] & 0xFF) <<  0) |
		        ((packet[40] & 0xFF) <<  8) |
		        ((packet[39] & 0xFF) << 16) |
		        ((packet[38] & 0xFF) << 24);
		    return value;
	}
	
	private boolean isAlpha(String s) {
		s = s.toUpperCase();
		for (int i = 0; i < s.length(); i++) {
			int c = (int) s.charAt(i);
			if (c < 65 || c > 90)
				return false;
		}
		return true;
	}

	// message handler
	public void onReceive(Object message) {
		//logger.info(message.toString());
		if (message instanceof byte[]) {
			// String work = (String) message;
			// if ("Thieves! thieves!".equals(work)) {
			// try {
			// logger.info("*** sleeping!");
			// Thread.sleep(5000);
			// logger.info("*** back!");
			// }
			// catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			// }
			//
			// // perform the work
			// List<Result> list = evaluateExpression(work);
			List<Result> list = evaluateExpression((byte[]) message);
			//if (!list.isEmpty()){
			// reply with the result
			actor.tell(list, getSelf());
			//}

		} else
			throw new IllegalArgumentException("Unknown message [" + message
					+ "]");
	}
}
