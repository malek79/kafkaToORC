package com.tcb.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ProducerFromTsv {
	final static String inputFilepath = "/home/malek/workspace/writeToOrcWithKafka/input/input.tsv";
	private static Properties props = new Properties();
	private static Producer<String, String> kafkaproducer;
	private static void configure(String servers) {

		props.put("bootstrap.servers", servers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaproducer = new KafkaProducer<String, String>(props);
	}

	public static void main(String[] args) throws Exception {
		
		ObjectMapper mapper= new ObjectMapper(new YAMLFactory());
		com.tcb.kafka.ConsumerConfig config = mapper.readValue(new File("/home/malek/workspace/writeToOrcWithKafka/properties/consumer.yaml"), com.tcb.kafka.ConsumerConfig.class);
		configure(config.getKafka().get("bootstrapServers"));
		
		BufferedReader TSVFile = new BufferedReader(new FileReader(inputFilepath));
		String dataRow = TSVFile.readLine(); // read first line
		while (( dataRow = TSVFile.readLine()) != null) {
			kafkaproducer.send(new ProducerRecord<String, String>(config.getKafka().get("topicName"), dataRow));
		System.out.println(dataRow);
		} 
		System.out.println("close producer");
		// Close the file once all data has been read.
		TSVFile.close();
	}
}
