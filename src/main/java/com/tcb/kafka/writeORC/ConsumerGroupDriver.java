package com.tcb.kafka.writeORC;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ConsumerGroupDriver {
	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		//Number of consumers
		int numConsumers = 3;
		// get kafka properties from yaml file
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		com.tcb.kafka.ConsumerConfig consumerConf = mapper.readValue(
				new File("/home/malek/workspace/writeToOrcWithKafka/properties/consumer.yaml"),
				com.tcb.kafka.ConsumerConfig.class);
		
		String groupId = consumerConf.getKafka().get("consumerGroup");
		List<String> topics = Arrays.asList(consumerConf.getKafka().get("topicName"));

		ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		
		final List<ConsumerGroupLoop> consumers = new ArrayList<>();
		
		for (int i = 0; i < numConsumers; i++) {
			ConsumerGroupLoop consumer = new ConsumerGroupLoop(i, groupId, topics);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerGroupLoop consumer : consumers) {
					try {
						
						consumer.shutdown();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		});
		
		
	}
}
