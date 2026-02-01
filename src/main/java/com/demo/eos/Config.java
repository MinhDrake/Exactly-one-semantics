package com.demo.eos;

import java.util.Properties;

public class Config {
  public static final String BOOTSTRAP_SERVERS = "localhost:29092";
  public static final String INPUT_TOPIC = "input-topic";
  public static final String OUTPUT_TOPIC = "output-topic";
  public static final String CONSUMER_GROUP_ID = "eos-processor-group";
  public static final String TRANSACTIONAL_ID = "eos-transactional-id";

  public static Properties getProducerProps(String transactionalId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("enable.idempotence", "true");
    if (transactionalId != null) {
      props.put("transactional.id", transactionalId);
    }
    return props;
  }

  public static Properties getConsumerProps(String groupId, String isolationLevel) {
    Properties props = new Properties();
    props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("group.id", groupId);
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false"); // Important for transactions
    props.put("isolation.level", isolationLevel); // read_committed or read_uncommitted
    return props;
  }
}
