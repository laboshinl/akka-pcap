package ru.laboshinl.pcap.akka.client;

import javax.swing.JFileChooser;

import akka.actor.*;
import akka.kernel.*;

import com.typesafe.config.*;

public class Client implements Bootable {
	public Client() {
		
		//String[] fileNames = {"split.001","split.002","split.003","split.004", "split.005"};
		
//		JFileChooser fileopen = new JFileChooser("/home/laboshinl/workspace/akka-mapreduce-example/src/main/resources");
//		int ret = fileopen.showDialog(null, "Открыть файл");                
//		if (ret == JFileChooser.APPROVE_OPTION) {
//		    fileName = fileopen.getSelectedFile().getName();
//		}



		ActorSystem system = ActorSystem.create("ClientApplication",
				ConfigFactory.load().getConfig("MapReduceClientApp"));


		String remotePath = "akka.tcp://MapReduceApp@127.0.0.1:2552/user/masterActor";
		ActorRef clientActor = system.actorOf(Props.create(ClientActor.class, remotePath));

		for (int i = 0; i < 10; i++){
		//final ActorRef fileReadActor = system.actorOf(Props.create(
		//		PcapReadActor.class));
			system.actorOf(Props.create(
					PcapReadActor.class)).tell("split.00"+ i, clientActor);
//			PcapReadActor.class)).tell("/bigFlows.pcap", clientActor);
		}
		for (int i = 10; i < 36; i++){
		//final ActorRef fileReadActor = system.actorOf(Props.create(
		//		PcapReadActor.class));
			system.actorOf(Props.create(
					PcapReadActor.class)).tell("split.0"+ i, clientActor);
//			PcapReadActor.class)).tell("/bigFlows.pcap", clientActor);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Client();
	}

	public void shutdown() {
		// TODO Auto-generated method stub
	}

	public void startup() {
		// TODO Auto-generated method stub
	}
}
