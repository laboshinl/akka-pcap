package ru.laboshinl.pcap.akka.main;

import java.io.IOException;


import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import ru.laboshinl.pcap.akka.client.Client;
import ru.laboshinl.pcap.akka.server.MapReduceServer;

public class ApplicationMain {

	public static void main(String[] args) throws FileDoesNotExistException, IOException, AlluxioException {
		new MapReduceServer();
		new Client();
    }
}
