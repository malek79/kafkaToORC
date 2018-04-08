package com.tcb.kafka.writeORC;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.orc.TypeDescription;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ConsumerGroupLoop implements Runnable {
	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;
	VectorizedRowBatch batch;
	SingleWriter singleWriter = SingleWriter.getInstance();
	private final static Properties kafkaProps = new Properties();

	ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

	com.tcb.kafka.ConsumerConfig consumerConf = mapper.readValue(
			new File("/home/malek/workspace/writeToOrcWithKafka/properties/consumer.yaml"),
			com.tcb.kafka.ConsumerConfig.class);

	private static void configure(String servers, String groupId) {

		kafkaProps.put("group.id", groupId);
		kafkaProps.put("bootstrap.servers", servers);
		kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

		kafkaProps.put("enable.auto.commit", "false");
		kafkaProps.put("auto.commit.interval.ms", "30000");
		kafkaProps.put("auto.offset.reset", "earliest");
		kafkaProps.put("session.timeout.ms", "30000");
		kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	}

	public static Object[] getKeysFromJson() throws FileNotFoundException, IOException, ParseException {
		FileReader file = new FileReader(
				"/home/malek/workspace/writeToOrcWithKafka/src/main/resources/orcTemplate.json");

		Object obj = new JSONParser().parse(file);
		// typecasting obj to JSONObject
		JSONObject jo = (JSONObject) obj;
		Object[] keys = jo.keySet().toArray();
		file.close();
		return keys;
	}

	public ConsumerGroupLoop(int id, String groupId, List<String> topics)
			throws JsonParseException, JsonMappingException, IOException {
		this.id = id;
		this.topics = topics;
		configure(consumerConf.getKafka().get("bootstrapServers"), groupId);
		this.consumer = new KafkaConsumer<String, String>(kafkaProps);
	}

	public static String getStructOrcFromJson() throws FileNotFoundException, IOException, ParseException {
		// parsing file "JSONExample.json"
		FileReader file = new FileReader(
				"/home/malek/workspace/writeToOrcWithKafka/src/main/resources/orcTemplate.json");

		Object obj = new JSONParser().parse(file);
		// typecasting obj to JSONObject
		JSONObject jo = (JSONObject) obj;
		Object[] keys = jo.keySet().toArray();
		String struture = "struct<";
		for (int i = 0; i < keys.length - 1; i++) {
			Map value1 = ((Map) jo.get(keys[i]));
			struture = struture + value1.get("name") + ":" + value1.get("type") + ",";
		}
		Map value1 = ((Map) jo.get(keys[keys.length - 1]));
		struture = struture + value1.get("name") + ":" + value1.get("type");
		struture = struture + ">";
		file.close();
		return struture;
	}

	@Override
	public void run() {
		try {

			consumer.subscribe(Arrays.asList(consumerConf.getKafka().get("topicName")));

			TypeDescription schema = TypeDescription.fromString(getStructOrcFromJson());

			batch = schema.createRowBatch();

			while (true) {
				// execution instant

				Instant previous = Instant.now();
				long debutSec = LocalDateTime.now().getSecond();

				// instant when fetching consumer Records
				Instant current;

				long timeSliceSeconds = consumerConf.getTimeSliceMinutes() * 60;

				long gap = 0;
				long runTime = timeSliceSeconds - debutSec;
				System.out.println("RunTime : " + runTime);
				// Runtime.getRuntime().addShutdownHook(thread);
				singleWriter.createFile();

				while (gap < runTime) {
					// optional
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}

					ConsumerRecords<String, String> records = consumer.poll(1000);

					SingleWriter.writeInOrcFile(records, batch);

					// instant when ends the first loop
					current = Instant.now();
					gap = ChronoUnit.SECONDS.between(previous, current);
					System.out.println("gap : " + gap);
					//
					if (records.count() == 0) {
						System.out.println("No more records to write.");
						consumer.close();
						break;
					}

				}
				singleWriter.destroyWriter();
				consumer.commitAsync();

			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public void shutdown() throws IOException {
		singleWriter.shutdownCloseWriter(batch);
		consumer.commitAsync();
		consumer.close();
	}

}
