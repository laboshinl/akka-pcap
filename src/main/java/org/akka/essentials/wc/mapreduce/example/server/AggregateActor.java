package org.akka.essentials.wc.mapreduce.example.server;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.akka.essentials.wc.mapreduce.example.common.*;

import com.google.common.primitives.Bytes;

import akka.actor.*;
import akka.event.*;

public class AggregateActor extends UntypedActor {
	final LoggingAdapter logger = Logging.getLogger(getContext().system(), this);

	private int completedTasksCount = 0;
	private TaskInfo taskInfo = null;
	private SortedMap<String, List<Byte>> finalReducedMap = new TreeMap<String, List<Byte>>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Map) {
			completedTasksCount++;
			@SuppressWarnings("unchecked")
			Map<String, List<Byte>> reducedList = (Map<String, List<Byte>>) message;
			aggregateInMemoryReduce(reducedList);
		}
		else if (message instanceof TaskInfo) {
			taskInfo = (TaskInfo) message;
		}

		// final outcome
		logger.info("completedTasksCount=" + completedTasksCount);
		if (taskInfo != null)
			logger.info("taskInfo#numberOfTasks=" + taskInfo.getNumberOfTasks());
		if (taskInfo != null && completedTasksCount >= taskInfo.getNumberOfTasks()) {
			PrintStream out = null;
			Iterator entries = finalReducedMap.entrySet().iterator();
			while (entries.hasNext()) {
			  Entry thisEntry = (Entry) entries.next();
			  //Object key = thisEntry.getKey();
			  //FileUtils.writeByteArrayToFile(new File("pathname"), myByteArray)
//				try {
              out = new PrintStream(new FileOutputStream((String) thisEntry.getKey()+".raw"));
              out.write(Bytes.toArray((List<Byte>) thisEntry.getValue()));
//			  //out.print(thisEntry.getValue());
//				}
//			  //Object value = thisEntry.getValue();
//			  // ...
//				finally {
//					if (out != null)
//						out.close();
//				}
			}
//			try {
//				out = new PrintStream(new FileOutputStream("finaloutput.log"));
//				out.print(finalReducedMap.lastKey());
//			}
//			finally {
//				if (out != null)
//					out.close();
//			}

			logger.error("*** now is really done!");
		}
	}

	private void aggregateInMemoryReduce(Map<String, List<Byte>> reducedList) {
		Iterator<String> iter = reducedList.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			if (finalReducedMap.containsKey(key)) {
				List<Byte> newList = new ArrayList<Byte>();
				newList.addAll(reducedList.get(key));
				newList.addAll(finalReducedMap.get(key));
				finalReducedMap.put(key, newList);
			}
			else {
				finalReducedMap.put(key, reducedList.get(key));
			}

		}
	}
}
