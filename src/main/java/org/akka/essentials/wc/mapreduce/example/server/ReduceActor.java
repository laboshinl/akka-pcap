package org.akka.essentials.wc.mapreduce.example.server;

import java.util.*;
import java.util.concurrent.*;

import com.google.common.primitives.Bytes;

import akka.actor.*;
import akka.event.*;

public class ReduceActor extends UntypedActor {
	final LoggingAdapter logger = Logging.getLogger(getContext().system(), this);

	private ActorRef aggregateActor = null;

	public ReduceActor(ActorRef aggregateActor) {
		this.aggregateActor = aggregateActor;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		logger.info(message.toString());
		if (message instanceof List) {
			@SuppressWarnings("unchecked")
			List<Result> work = (List<Result>) message;

			// perform the work
			NavigableMap<String, List<Byte>> reducedList = reduce(work);

			// reply with the result
			aggregateActor.tell(reducedList, getSelf());
		}
		else
			throw new IllegalArgumentException("Unknown message [" + message + "]");
	}

	private NavigableMap<String, Integer> reduce2(List<Result> list) {
		NavigableMap<String, Integer> reducedMap = new ConcurrentSkipListMap<String, Integer>();

		Iterator<Result> iter = list.iterator();
		while (iter.hasNext()) {
			Result result = iter.next();
			if (reducedMap.containsKey(result.getWord())) {
				Integer value = (Integer) reducedMap.get(result.getWord());
				value++;
				reducedMap.put(result.getWord(), value);
			}
			else {
				reducedMap.put(result.getWord(), Integer.valueOf(1));
			}
		}
		return reducedMap;
	}
	private NavigableMap<String, List<Byte> > reduce(List<Result> list) {
		NavigableMap<String, List<Byte> > reducedMap = new ConcurrentSkipListMap<String, List<Byte>>();

		Iterator<Result> iter = list.iterator();
		while (iter.hasNext()) {
			Result result = iter.next();
			if (reducedMap.containsKey(result.getWord())) {
				List<Byte> value = reducedMap.get(result.getWord());
				value.addAll(Bytes.asList(result.getData()));
				reducedMap.put(result.getWord(), value);
			}
			else {
				reducedMap.put(result.getWord(),  Bytes.asList(result.getData()));
			}
		}
		return reducedMap;
	}
}
