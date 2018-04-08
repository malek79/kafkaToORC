package com.tcb.kafka.writeORC.singleton;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ThreadInfo;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.hadoop.conf.*;

public class SingleWriter {

	private static SingleWriter singleton;
	private static Writer writer;
	private static Thread threadin;
	private static TypeDescription schema;
	private static String outfilename;
	private static AtomicBoolean myBoolean = new AtomicBoolean(false);
	private static AtomicBoolean mycloseBoolean = new AtomicBoolean(false);

	// private static VectorizedRowBatch batch;

	private SingleWriter() {

	}

	public static SingleWriter getInstance() throws IllegalArgumentException, IOException {

		if (singleton == null) {

			singleton = new SingleWriter();
			singleton.createFile();

		}
		return singleton;
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

	public Writer createFile() throws IllegalArgumentException, IOException {
		System.out.println("Thread : " + Thread.currentThread().getName());

		if (myBoolean.compareAndSet(false, true)) {
			// myBoolean.compareAndSet(false, true);
			threadin = Thread.currentThread();
			System.out.println("Threadin : " + Thread.currentThread().getName());
			LocalDateTime now = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH-mm-ss");

			outfilename = "/home/malek/workspace/writeToOrcWithKafka/output/ORCFile" + now.format(formatter) + ".orc";

			Configuration conf = new Configuration();
			try {
				schema = TypeDescription.fromString(getStructOrcFromJson());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			// Get the schema of the types in the ORC file

			writer = OrcFile.createWriter(new Path(outfilename), OrcFile.writerOptions(conf).setSchema(schema));
			mycloseBoolean.set(false);

			return writer;
		}

		return null;
	}

	public static void writeInOrcFile(KafkaConsumer<String, String> consumer, int id)
			throws IOException, NumberFormatException, ParseException {

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

		com.tcb.kafka.ConsumerConfig consumerConf = mapper.readValue(
				new File("/home/malek/workspace/writeToOrcWithKafka/properties/consumer.yaml"),
				com.tcb.kafka.ConsumerConfig.class);

		while (true) {
			// execution instant

			Instant previous = Instant.now();
			long debutSec = LocalDateTime.now().getSecond();

			// instant when fetching consumer Records
			Instant current;

			long timeSliceSeconds = consumerConf.getTimeSliceMinutes() * 60;
		
			singleton.createFile();

			VectorizedRowBatch batch = schema.createRowBatch();

			// thread
			// Thread thread = getThread(singleWriter.getWriter(), batch);

			long gap = 0;
			long runTime = timeSliceSeconds - debutSec;
			System.out.println("RunTime : " + runTime);
			// Runtime.getRuntime().addShutdownHook(thread);

			while (gap < runTime) {
				// optional
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}

				ConsumerRecords<String, String> records = consumer.poll(1000);

				LongColumnVector intvector = (LongColumnVector) batch.cols[0];
				BytesColumnVector stringVector = (BytesColumnVector) batch.cols[1];

				System.out.println("Threadin cons : " + threadin.getState());
				System.out.println("consumer : " + Thread.currentThread().getName() + ":::" + "offset : "
						+ records.iterator().next().offset());
				for (ConsumerRecord<String, String> record : records) {

					int row = batch.size++;
					intvector.vector[row] = Integer
							.valueOf(record.value().split("\\t")[Integer.valueOf(getKeysFromJson()[0].toString())]);
					stringVector.setVal(row,
							record.value().split("\\t")[Integer.valueOf(getKeysFromJson()[1].toString())].getBytes());
					if (batch.size >= batch.getMaxSize() - 100) {
						synchronized (writer) {
							writer.addRowBatch(batch);
						}

						batch.reset();
					}
				}
				// writer.addRowBatch(batch);
				if (batch.size >= batch.getMaxSize() - 100) {

					synchronized (writer) {
						writer.addRowBatch(batch);
					}

					batch.reset();
				}
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
			singleton.destroyWriter();
			consumer.commitAsync();
		}
	}

	public void destroyWriter() throws IOException {

		if (mycloseBoolean.compareAndSet(false, true)) {
			// mycloseBoolean.compareAndSet(false, true);

			writer.close();
			myBoolean.set(false);
			System.out.println("Wrote " + writer.getNumberOfRows() + " records to ORC file " + outfilename);
		}

		// singleton.createFile(id);
		//
		// System.out.println("new file");

	}

	public Writer getWriter() {
		return writer;
	}
}
