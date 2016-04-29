package ru.laboshinl.pcap.akka.server;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import ru.laboshinl.pcap.akka.common.*;

import com.google.common.primitives.Bytes;

import akka.actor.*;
import akka.event.*;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.Version;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

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
//			FileOutStream out = null;

			Iterator entries = finalReducedMap.entrySet().iterator();
			while (entries.hasNext()) {
				Entry thisEntry = (Entry) entries.next();
				try {	
//					AlluxioURI path = new AlluxioURI("/"+(String) thisEntry.getKey() + ".raw");
//					FileSystem fs = FileSystem.Factory.get();				
//					out = fs.createFile(path);
					out = new PrintStream(new FileOutputStream(
							(String) thisEntry.getKey() + ".raw"));
					Map<Integer, List<Byte>> treeMap = new TreeMap<Integer, List<Byte>>(
							(Map<Integer, List<Byte>>) thisEntry.getValue());
					for (Integer key : treeMap.keySet()) {					
						out.write(Bytes.toArray((List<Byte>) treeMap.get(key)));
//						out.write(Bytes.toArray((List<Byte>) treeMap.get(key)));
					}
					
					
					// Create a file and get its output stream

					// Write data

					// Close and complete file
					//out.close();



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
