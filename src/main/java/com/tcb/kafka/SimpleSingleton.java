package com.tcb.kafka;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
//Seriously why do I have to import 3 things so get the date. Java = superflous objects everywhere
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.simple.parser.ParseException;

public final class SimpleSingleton{
	//Implements a singleton logger instance
	private static final SimpleSingleton instance = new SimpleSingleton();

	//Retrieve the execution directory. Note that this is whereever this process was launched
	public String logname = "OrcFile";
	protected String env = "/home/malek/workspace/writeToOrcWithKafka/output";
	private static Writer writer ;
	private static VectorizedRowBatch batch ;
	public static SimpleSingleton getInstance(){
		return instance;
	}

	public static SimpleSingleton getInstance(String withName){
		instance.logname = withName;
		instance.createFile();
		return instance;
	}

	public void createFile(){
		//Determine if a logs directory exists or not.
		//Get the current date and time
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	   	Calendar cal = Calendar.getInstance();
	   	
	   	//Create the name of the file from the path and current time
		logname =  logname + '-' +  dateFormat.format(cal.getTime()) + ".orc";
		Configuration conf = new Configuration();
		// Get the schema of the types in the ORC file
		TypeDescription schema = TypeDescription.fromString("struct<refer:int,name:string>");
		try {
			writer=OrcFile.createWriter(new Path(env+"/"+logname), OrcFile.writerOptions(conf).setSchema(schema));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//SimpleLog.logFile = new File(logsFolder.getName(),logname);
	
	}

	private SimpleSingleton(){
		if (instance != null){
			//Prevent Reflection
			throw new IllegalStateException("Cannot instantiate a new singleton instance of log");
		}
		this.createFile();
	}

	public static void write(ConsumerRecords<String, String> records)	throws IOException, NumberFormatException, ParseException {
		LongColumnVector intvector = (LongColumnVector) batch.cols[0];
		BytesColumnVector stringVector = (BytesColumnVector) batch.cols[1];
		for (ConsumerRecord<String, String> record : records) {

			int row = batch.size++;
			intvector.vector[row] = Integer
					.valueOf(record.value().split("\\t")[1]);
			stringVector.setVal(row,
					record.value().split("\\t")[4].getBytes());
			if (batch.size == batch.getMaxSize()) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		}
		writer.addRowBatch(batch);
		//batch.reset();
	
	}

	public static void main(String[] args) {
		
		//SimpleLog.log("This is a message");
	}

}