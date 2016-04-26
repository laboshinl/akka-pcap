package ru.laboshinl.pcap.akka.main;

import ru.laboshinl.pcap.akka.client.Client;
import ru.laboshinl.pcap.akka.server.MapReduceServer;

public class ApplicationMain {

	public static void main(String[] args) {
		new MapReduceServer();
		new Client();
    }
}
