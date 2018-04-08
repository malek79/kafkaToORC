package com.tcb.kafka;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
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
	private final static Properties kafkaProps = new Properties();
	SingleWriter singleWriter = SingleWriter.getInstance();
	ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

	com.tcb.kafka.ConsumerConfig consumerConf = mapper.readValue(
			new File("/home/malek/workspace/writeToOrcWithKafka/properties/consumer.yaml"),
			com.tcb.kafka.ConsumerConfig.class);

	private static void configure(String servers, String groupId) {

		kafkaProps.put("group.id", groupId);
		kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		kafkaProps.put("bootstrap.servers", servers);
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

	public Thread getThread(Writer writer, VectorizedRowBatch batch) {
		Thread thread = new Thread() {
			public void run() {
				System.out.println("bye...." + batch.count());
				consumer.commitAsync();
				consumer.close();
				try {
					if (batch.count() != 0)
						writer.addRowBatch(batch);
					writer.close();
					System.out.println("Nombre of rows wrotes in files " + writer.getNumberOfRows());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		return thread;
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

			while (true) {
				// execution instant
				
				Instant previous = Instant.now();
				System.out.println("previous : " + previous);
				long debutSec = LocalDateTime.now().getSecond();

				// instant when fetching consumer Records
				Instant current;

				long timeSliceSeconds = consumerConf.getTimeSliceMinutes() * 60;

				

				// Get the schema of the types in the ORC file
				TypeDescription schema = TypeDescription.fromString(getStructOrcFromJson());

				VectorizedRowBatch batch = schema.createRowBatch();

				// thread
				// Thread thread = getThread(singleWriter.getWriter(), batch);

				long gap = 0;
				System.out.println("gap : " + gap);
				System.out.println("debut sec : " + debutSec);
				long runTime = timeSliceSeconds - debutSec;
				System.out.println("ReunTime : " + runTime);
				//Runtime.getRuntime().addShutdownHook(thread);

				while (gap < runTime) {
					// optional
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					ConsumerRecords<String, String> records = consumer.poll(1000);
					
					System.out.println("offset : " + records.iterator().next().offset());
					System.out.println("consumer id : " + id);
					writeInOrcFile(records, batch, singleWriter.getWriter());
					
					// instant when ends the first loop
					current = Instant.now();
					System.out.println("current2 : " + current);
					gap = ChronoUnit.SECONDS.between(previous, current);
					System.out.println("gap2 : " + gap);
					//
					if (records.count() == 0) {
						System.out.println("No more records to write.");
						consumer.close();
						break;
					}

				}
				System.out.println("close orc writer");
				singleWriter.destroyWriter();
				consumer.commitAsync();

			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
	
	private static void writeInOrcFile(ConsumerRecords<String, String> records, VectorizedRowBatch batch, Writer writer)
			throws IOException, NumberFormatException, ParseException {
		LongColumnVector intvector = (LongColumnVector) batch.cols[0];
		BytesColumnVector stringVector = (BytesColumnVector) batch.cols[1];
		System.out.println("1");
		for (ConsumerRecord<String, String> record : records) {

			int row = batch.size++;
			intvector.vector[row] = Integer
					.valueOf(record.value().split("\\t")[Integer.valueOf(getKeysFromJson()[0].toString())]);
			stringVector.setVal(row,
					record.value().split("\\t")[Integer.valueOf(getKeysFromJson()[1].toString())].getBytes());
			if (batch.size >= batch.getMaxSize()-100) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		}
		System.out.println("2");
	//	writer.addRowBatch(batch);
		System.out.println("batch size   " + batch.size);
		if (batch.size >= batch.getMaxSize()-100) {
			writer.addRowBatch(batch);
			batch.reset();
		}
		System.out.println("3");
	}

	public void shutdown() {
		consumer.close();
	}

}
