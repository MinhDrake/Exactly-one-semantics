package com.demo.eos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class OutputVerifier {
  public static void main(String[] args) {
    // IMPORTANT: Must use "read_committed" to avoid seeing aborted transaction
    // garbage
    var props = Config.getConsumerProps("eos-verifier-group", "read_committed");
    var consumer = new KafkaConsumer<String, String>(props);

    consumer.subscribe(Collections.singletonList(Config.OUTPUT_TOPIC));

    System.out.println("Starting Verifier. Listening for valid committed output...");

    Set<String> processedKeys = new HashSet<>();
    int duplicateCount = 0;

    while (true) {
      var records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        if (processedKeys.contains(record.key())) {
          System.err.println("❌ DUPLICATE DETECTED! Key: " + record.key() + " Value: " + record.value());
          duplicateCount++;
        } else {
          processedKeys.add(record.key());
          System.out.println("✅ Received Valid: " + record.key() + " -> " + record.value());
        }
      }
    }
  }
}
