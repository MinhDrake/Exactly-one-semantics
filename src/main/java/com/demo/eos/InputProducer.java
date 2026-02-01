package com.demo.eos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.stream.IntStream;

public class InputProducer {
  public static void main(String[] args) {
    var props = Config.getProducerProps(null); // No transaction needed for input source
    try (var producer = new KafkaProducer<String, String>(props)) {
      System.out.println("Generating 10 integers...");
      IntStream.range(1, 11).forEach(i -> {
        String key = "key-" + i;
        String value = String.valueOf(i);
        producer.send(new ProducerRecord<>(Config.INPUT_TOPIC, key, value));
        System.out.println("Sent: " + value);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      });
      System.out.println("Done sending input.");
    }
  }
}
