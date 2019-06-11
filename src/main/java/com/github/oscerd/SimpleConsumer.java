package com.github.oscerd;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.core.JsonProcessingException;

public class SimpleConsumer {

	public static void main(String[] args) throws JsonProcessingException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("group.id", UUID.randomUUID().toString());

		KafkaConsumer<String, String> cons = new KafkaConsumer<String, String>(props);
		List<String> topics = new ArrayList<String>();
		topics.add(args[0]);
		cons.subscribe(topics);

		ConsumerRecords<String, String> allRecords = cons.poll(Duration.ofMillis(5000L));
		
        Iterator<ConsumerRecord<String, String>> it = allRecords.iterator();
        for (Iterator iterator = allRecords.iterator(); iterator.hasNext();) {
			ConsumerRecord<String, String> rec = (ConsumerRecord<String, String>) iterator.next();
			for (Iterator iterator2 = rec.headers().iterator(); iterator2.hasNext();) {
				Header header = (Header) iterator2.next();
				System.err.println(new String(header.value()));
				
			}
		}

		cons.close();
	}
}
