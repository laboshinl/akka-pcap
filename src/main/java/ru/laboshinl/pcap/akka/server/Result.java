package ru.laboshinl.pcap.akka.server;

import java.io.Serializable;

public class Result implements Serializable{
	private static final long serialVersionUID = 5727560172917790458L;
	private String word;
	private int no_of_instances;
	private byte[] data;
	private long sequence;

	public Result(String word, int no_of_instances){
		this.setWord(word);
		this.setNoOfInstances(no_of_instances);
	}

	public void setWord(String word) {
		this.word = word;
	}

	public String getWord() {
		return word;
	}
	
	public void setSequence(long sequence){
		this.sequence = sequence;
	}
	
	public long getSequence(){
		return sequence;
	}
	
	public void setData(byte[] data) {
		this.data = data;
	}

	public byte[] getData() {
		return data;
	}


	public void setNoOfInstances(int no_of_instances) {
		this.no_of_instances = no_of_instances;
	}

	public int getNoOfInstances() {
		return no_of_instances;
	}

	public String toString(){
		return word + ":" + no_of_instances;
	}

}