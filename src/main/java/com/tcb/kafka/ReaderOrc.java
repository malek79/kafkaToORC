package com.tcb.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

public class ReaderOrc {

	public static void main(String [ ] args) throws java.io.IOException
	{
		Configuration conf = new Configuration();
		Reader reader = OrcFile.createReader(new Path("/home/malek/workspace/writeToOrcWithKafka/output/ORCFile20170928-16-25-00.orc"),
				OrcFile.readerOptions(conf));
		RecordReader rows = reader.rows();
		VectorizedRowBatch batch = reader.getSchema().createRowBatch();
		int r =0;
		while (rows.nextBatch(batch)) {
			LongColumnVector  intVector = (LongColumnVector) batch.cols[0];
			BytesColumnVector stringVector = (BytesColumnVector)  batch.cols[1];
		for(int i=0; i < batch.size; i++) {
			int intValue = (int) intVector.vector[i];
			String stringValue = new String(stringVector.vector[i], stringVector.start[i], stringVector.length[i]);
			r++;
		//	System.out.println(intValue + ", " + stringValue);
		
			}
			}
		System.out.println("Rows in file :"+r);
		rows.close();
	}

}
