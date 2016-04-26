package ru.laboshinl.pcap.akka.server;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import ru.laboshinl.pcap.akka.common.*;

import com.google.common.primitives.Bytes;

import akka.actor.*;
import akka.event.*;

public class AggregateActor extends UntypedActor {
	final LoggingAdapter logger = Logging
			.getLogger(getContext().system(), this);

	private int completedTasksCount = 0;
	private TaskInfo taskInfo = null;
	private SortedMap<String, Map<Integer, List<Byte>>> finalReducedMap = new TreeMap<String, Map<Integer, List<Byte>>>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Map) {
			completedTasksCount++;
			@SuppressWarnings("unchecked")
			Map<String, Map<Integer, List<Byte>>> reducedList = (Map<String, Map<Integer, List<Byte>>>) message;
			aggregateInMemoryReduce(reducedList);
		} else if (message instanceof TaskInfo) {
			taskInfo = (TaskInfo) message;
		}

		// final outcome
		logger.info("completedTasksCount=" + completedTasksCount);
		if (taskInfo != null)
			logger.info("taskInfo#numberOfTasks=" + taskInfo.getNumberOfTasks());
		if (taskInfo != null
				&& completedTasksCount >= taskInfo.getNumberOfTasks()) {
			PrintStream out = null;

			Iterator entries = finalReducedMap.entrySet().iterator();
			while (entries.hasNext()) {
				Entry thisEntry = (Entry) entries.next();
				try {
					out = new PrintStream(new FileOutputStream(
							(String) thisEntry.getKey() + ".raw"));
					Map<Integer, List<Byte>> treeMap = new TreeMap<Integer, List<Byte>>(
							(Map<Integer, List<Byte>>) thisEntry.getValue());
					for (Integer key : treeMap.keySet()) {
						out.write(Bytes.toArray((List<Byte>) treeMap.get(key)));
					}

				} finally {
					if (out != null)
						out.close();
				}
			}

			logger.error("*** now is really done!");
		}
	}

	private void aggregateInMemoryReduce(
			Map<String, Map<Integer, List<Byte>>> reducedList) {
		Iterator<String> iter = reducedList.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			if (finalReducedMap.containsKey(key)) {
				finalReducedMap.get(key).putAll(reducedList.get(key));
			} else {
				finalReducedMap.putAll(reducedList);
			}

		}
	}
}
