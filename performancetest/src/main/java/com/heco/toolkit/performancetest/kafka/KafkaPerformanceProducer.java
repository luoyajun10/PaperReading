package com.heco.toolkit.performancetest.kafka;

/**
 * Kafka Performance Test Producer
 */
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

public class KafkaPerformanceProducer extends Thread {

	private final Logger logger = Logger.getLogger(KafkaPerformanceProducer.class);

	private final Producer<Integer, String> producer;
	private final String topic;
	private final int numOfMessage;
	private final int sendInterval;
	private final Properties props = new Properties();

	public KafkaPerformanceProducer(String kafkaBrokerList,String topic,int numOfMessage, int sendInterval) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", kafkaBrokerList);//"192.168.220.134:9092"
		//async send
		props.put("producer.type", "async");
		//the message number each sending
		props.put("batch.num.messages", "5");
		producer = new KafkaProducer<Integer,String>(props);
		
		this.topic = topic;
		this.numOfMessage = numOfMessage;
		this.sendInterval = sendInterval;
	}

	/**
	 * run
	 */
	public void run() {
		long startTime = System.currentTimeMillis();
		int messageNo = 1;
		while(messageNo <= numOfMessage) {
			String message = "2023090810|FlowNo|JOB_ID|SERVICE_NAME|CONSUMER_SEQ_NO";
			ProducerRecord<Integer,String> messageForSend = new ProducerRecord<Integer,String>(topic, message);
			producer.send(messageForSend);

			try {
				sleep(sendInterval);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			messageNo++;
		}
		producer.close();
		long endTime = System.currentTimeMillis();
		logger.info("Thread [" + this.getId() + "] run over. Time use: " + (endTime - startTime));
	}

}
