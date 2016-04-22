package org.akka.essentials.wc.mapreduce.example.server;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.Hex;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.*;

public class MapActor extends UntypedActor {
	final LoggingAdapter logger = Logging
			.getLogger(getContext().system(), this);

	String[] STOP_WORDS = { "a", "about", "above", "above", "across", "after",
			"afterwards", "again", "against", "all", "almost", "alone",
			"along", "already", "also", "although", "always", "am", "among",
			"amongst", "amoungst", "amount", "an", "and", "another", "any",
			"anyhow", "anyone", "anything", "anyway", "anywhere", "are",
			"around", "as", "at", "back", "be", "became", "because", "become",
			"becomes", "becoming", "been", "before", "beforehand", "behind",
			"being", "below", "beside", "besides", "between", "beyond", "bill",
			"both", "bottom", "but", "by", "call", "can", "cannot", "cant",
			"co", "con", "could", "couldnt", "cry", "de", "describe", "detail",
			"do", "done", "down", "due", "during", "each", "eg", "eight",
			"either", "eleven", "else", "elsewhere", "empty", "enough", "etc",
			"even", "ever", "every", "everyone", "everything", "everywhere",
			"except", "few", "fifteen", "fify", "fill", "find", "fire",
			"first", "five", "for", "former", "formerly", "forty", "found",
			"four", "from", "front", "full", "further", "get", "give", "go",
			"had", "has", "hasnt", "have", "he", "hence", "her", "here",
			"hereafter", "hereby", "herein", "hereupon", "hers", "herself",
			"him", "himself", "his", "how", "however", "hundred", "ie", "if",
			"in", "inc", "indeed", "interest", "into", "is", "it", "its",
			"itself", "keep", "last", "latter", "latterly", "least", "less",
			"ltd", "made", "many", "may", "me", "meanwhile", "might", "mill",
			"mine", "more", "moreover", "most", "mostly", "move", "much",
			"must", "my", "myself", "name", "namely", "neither", "never",
			"nevertheless", "next", "nine", "no", "nobody", "none", "noone",
			"nor", "not", "nothing", "now", "nowhere", "of", "off", "often",
			"on", "once", "one", "only", "onto", "or", "other", "others",
			"otherwise", "our", "ours", "ourselves", "out", "over", "own",
			"part", "per", "perhaps", "please", "put", "rather", "re", "same",
			"see", "seem", "seemed", "seeming", "seems", "serious", "several",
			"she", "should", "show", "side", "since", "sincere", "six",
			"sixty", "so", "some", "somehow", "someone", "something",
			"sometime", "sometimes", "somewhere", "still", "such", "system",
			"take", "ten", "than", "that", "the", "their", "them",
			"themselves", "then", "thence", "there", "thereafter", "thereby",
			"therefore", "therein", "thereupon", "these", "they", "thickv",
			"thin", "third", "this", "those", "though", "three", "through",
			"throughout", "thru", "thus", "to", "together", "too", "top",
			"toward", "towards", "twelve", "twenty", "two", "un", "under",
			"until", "up", "upon", "us", "very", "via", "was", "we", "well",
			"were", "what", "whatever", "when", "whence", "whenever", "where",
			"whereafter", "whereas", "whereby", "wherein", "whereupon",
			"wherever", "whether", "which", "while", "whither", "who",
			"whoever", "whole", "whom", "whose", "why", "will", "with",
			"within", "without", "would", "yet", "you", "your", "yours",
			"yourself", "yourselves", "the" };

	List<String> STOP_WORDS_LIST = Arrays.asList(STOP_WORDS);

	private ActorRef actor = null;

	public MapActor(ActorRef inReduceActor) {
		actor = inReduceActor;
	}

	private List<Result> evaluateExpression(String line) {
		List<Result> list = new ArrayList<Result>();
		StringTokenizer parser = new StringTokenizer(line);
		while (parser.hasMoreTokens()) {
			String word = parser.nextToken().toLowerCase();
			if (isAlpha(word) && !STOP_WORDS_LIST.contains(word)) {
				list.add(new Result(word, 1));
			}
		}
		return list;
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

	private List<Result> evaluateExpression2(byte[] packet) {
		List<Result> list = new ArrayList<Result>();
		//tcpPush(packet);
		if (ipProto(packet) == 6  && tcpPush(packet) == 1 ) {
			Result result = new Result(Long.toString((long) (ipDst(packet)
					+ ":" + Integer.toString(tcpDstPort(packet))).hashCode()
					+ (long) (ipSrc(packet) + ":" + Integer
							.toString(tcpSrcPort(packet))).hashCode()), 1);
			result.setData(Arrays.copyOfRange(packet, 54, packet.length));
			list.add(result);
			//logger.info("Flags " + tcpPush(packet));
		}

		return list;
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
			List<Result> list = evaluateExpression2((byte[]) message);
			//if (!list.isEmpty()){
			// reply with the result
			actor.tell(list, getSelf());
			//}

		} else
			throw new IllegalArgumentException("Unknown message [" + message
					+ "]");
	}
}
