package com.tcb.kafka;

import java.util.Map;

public class ConsumerConfig {
private int consumerThreadNumber;

private int workerThreadNumber ;
private String cacheFolder ;
private int  timeSliceMinutes;
private String pidLocation;
private String orcTemplatePath;
private Map<String,String> hdfs;
private Map<String,String> kafka ;
public int getConsumerThreadNumber() {
	return consumerThreadNumber;
} 
public void setConsumerThreadNumber(int consumerThreadNumber) {
	this.consumerThreadNumber = consumerThreadNumber;
}
public int getWorkerThreadNumber() {
	return workerThreadNumber;
}
public void setWorkerThreadNumber(int workerThreadNumber) {
	this.workerThreadNumber = workerThreadNumber;
}
public String getCacheFolder() {
	return cacheFolder;
}
public void setCacheFolder(String cacheFolder) {
	this.cacheFolder = cacheFolder;
}
public int getTimeSliceMinutes() {
	return timeSliceMinutes;
}
public void setTimeSliceMinutes(int timeSliceMinutes) {
	this.timeSliceMinutes = timeSliceMinutes;
}
public String getPidLocation() {
	return pidLocation;
}
public void setPidLocation(String pidLocation) {
	this.pidLocation = pidLocation;
}
public String getOrcTemplatePath() {
	return orcTemplatePath;
}
public void setOrcTemplatePath(String orcTemplatePath) {
	this.orcTemplatePath = orcTemplatePath;
}
public Map<String, String> getHdfs() {
	return hdfs;
}
public void setHdfs(Map<String, String> hdfs) {
	this.hdfs = hdfs;
}
public Map<String, String> getKafka() {
	return kafka;
}
public void setKafka(Map<String, String> kafka) {
	this.kafka = kafka;
}

}
