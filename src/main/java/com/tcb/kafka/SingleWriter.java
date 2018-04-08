package com.tcb.kafka;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Map;

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
	private TypeDescription schema;
	private String outfilename;
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
	/*private static Object[] getKeysFromJson() throws FileNotFoundException, IOException, ParseException {
		FileReader file = new FileReader(
				"/home/malek/workspace/writeToOrcWithKafka/src/main/resources/orcTemplate.json");

		Object obj = new JSONParser().parse(file);
		// typecasting obj to JSONObject
		JSONObject jo = (JSONObject) obj;
		Object[] keys = jo.keySet().toArray();
		file.close();
		return keys;
	}*/

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

		return writer;
	}

	private static void writeInOrcFile(ConsumerRecords<String, String> records, VectorizedRowBatch batch)
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
	
	public void destroyWriter() throws IOException{
		writer.close();
		System.out.println("close writer");
		System.out.println(
				"Wrote " + writer.getNumberOfRows() + " records to ORC file " + outfilename);
		singleton.createFile();
		System.out.println("new file");
	}
	
	public Writer getWriter(){
		return writer;
	}
}
