package com.alibaba.otter.canal.common.utils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.alarm.ElongAlarmHandler;

public class KafkaUtil {
	private final static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);
	private Properties props;
	private ElongAlarmHandler canalAlarmHandler;

	private static class SingletonHolder {
		private static final KafkaUtil KAFKA_UTIL = new KafkaUtil();
	}

	private int producerThreadNum = 10;
	private KafkaProducer[] kafkaProducers = new KafkaProducer[producerThreadNum];
	private ExecutorService service = Executors.newFixedThreadPool(producerThreadNum);

	private KafkaUtil() {
		try {
			props = new Properties();
			props.load(KafkaUtil.class.getClassLoader().getResourceAsStream("kafka.properties"));
			for (int i = 0; i < producerThreadNum; i++) {
				KafkaProducer producer = new KafkaProducer<String, String>(props);
				kafkaProducers[i] = producer;
			}

			String o = props.getProperty("producerThreadNum", "10");
			if (o != null) {
				try {
					producerThreadNum = Integer.parseInt(o);
				} catch (NumberFormatException e) {
					logger.error("format kafka producerThreadNum error ,the value is {}", producerThreadNum);
				}
			}

			String alarmType = props.getProperty("alarmType", "2");
			String alarmReceiver = props.getProperty("alarmReceiver");
			if (alarmReceiver == null) {
				logger.error("the kafka  alarmReceiver is null ,please set it on kafka.properties ,system exit");
				System.exit(0);
			}

			canalAlarmHandler = new ElongAlarmHandler();
			canalAlarmHandler.setAlarmReceiver(alarmReceiver);
			try {
				canalAlarmHandler.setAlarmType(Integer.parseInt(alarmType));
			} catch (NumberFormatException e) {
				logger.error("format kafka alarmType error,the values is {}", alarmType);
			}
		} catch (IOException e) {
			logger.error("load kafka properties error ,the error is ", e);
		}

	}

	public static KafkaUtil instance() {
		return SingletonHolder.KAFKA_UTIL;
	}

	public void sentData(String topic, String key, String value) {
		ProducerRecord record = new ProducerRecord<String, String>(topic, key, value);
		kafkaProducers[key.hashCode() % producerThreadNum].send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					logger.error("sent to kafka error ,the error is ", exception);
					if (null != canalAlarmHandler) {
						canalAlarmHandler.sendAlarmElong("canal send kafka error", exception.getMessage());
					}
				} else {
					long offset = metadata.offset();
					if (offset % 10000 == 0) {
						logger.info("send to kafka, the offset is {},the partition is {}", offset, metadata.partition());
					}

				}
			}
		});

	}

	class SendThread implements Runnable {
		private KafkaProducer producer;
		private ProducerRecord record;

		public SendThread(KafkaProducer producer, ProducerRecord record) {
			this.producer = producer;
			this.record = record;
		}

		@Override
		public void run() {
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null) {
						logger.error("sent to kafka error ,the error is ", exception);
					} else {

					}
				}
			});

		}

	}
}
