package org.akka.essentials.wc.mapreduce.example.main;

import org.akka.essentials.wc.mapreduce.example.client.Client;
import org.akka.essentials.wc.mapreduce.example.server.MapReduceServer;

public class ApplicationMain {

	public static void main(String[] args) {
		new MapReduceServer();
		new Client();
    }
}
