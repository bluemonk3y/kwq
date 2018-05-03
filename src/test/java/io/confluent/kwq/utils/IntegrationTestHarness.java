/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kwq.utils;

import io.confluent.common.utils.TestUtils;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.kwq.util.KafkaTopicClient;
import io.confluent.kwq.util.KafkaTopicClientImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IntegrationTestHarness {

  private static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 5000;
  private final String CONSUMER_GROUP_ID_PREFIX  = "KWQ-test";

  public EmbeddedSingleNodeKafkaCluster embeddedKafkaCluster;
  private KsqlConfig ksqlConfig;
  private AdminClient adminClient;
  private KafkaTopicClient topicClient;


  public void start() throws Exception {
    embeddedKafkaCluster = new EmbeddedSingleNodeKafkaCluster();
    embeddedKafkaCluster.start();
    Map<String, Object> configMap = new HashMap<>();

    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaCluster.bootstrapServers());
    configMap.put("application.id", "KWQ");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    configMap.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

    this.ksqlConfig = new KsqlConfig(configMap);

    System.out.println("HARNESS:" + ksqlConfig.getKsqlAdminClientConfigProps());


    this.adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    this.topicClient = new KafkaTopicClientImpl(adminClient);
  }

  public void stop() {
    this.topicClient.close();
    this.adminClient.close();
    this.embeddedKafkaCluster.stop();
  }


  public void createTopic(String topicName, int numPartitions, int replicationFactor) {
    topicClient.createTopic(topicName, numPartitions, (short) replicationFactor);
  }

  public <V> Map<String, RecordMetadata> produceData(String topicName,
                                                 Map<String, V> recordsToPublish,
                                                 Serializer<V> valueSerializer,
                                                 Long timestamp)
          throws InterruptedException, TimeoutException, ExecutionException {

    createTopic(topicName, 1, 1);

    Properties producerConfig = properties();
    KafkaProducer<String, V> producer =
            new KafkaProducer<>(producerConfig, new StringSerializer(), valueSerializer);

    Map<String, RecordMetadata> result = new HashMap<>();
    for (Map.Entry<String, V> recordEntry : recordsToPublish.entrySet()) {
      Future<RecordMetadata> recordMetadataFuture = producer.send(buildRecord(topicName, timestamp, recordEntry));
      result.put(recordEntry.getKey(), recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }
    producer.close();

    return result;
  }
  private <V> ProducerRecord<String, V> buildRecord(String topicName,
                                                         Long timestamp,
                                                         Map.Entry<String, V> recordEntry) {
    return new ProducerRecord<>(topicName, null, timestamp,  recordEntry.getKey(), recordEntry.getValue());
  }

  private Properties properties() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            ksqlConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    return producerConfig;
  }

  void produceRecord(final String topicName, final String key, final String data) {
    try(final KafkaProducer<String, String> producer
                = new KafkaProducer<>(properties(),
            new StringSerializer(),
            new StringSerializer())) {
      producer.send(new ProducerRecord<>(topicName, key, data));
    }
  }


  public <K, V> Map<K, V> consumeData(String topic,
                                            int expectedNumMessages,
                                            Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer,
                                            long resultsPollMaxTimeMs) {

    topic = topic.toUpperCase();

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ksqlConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID_PREFIX + System.currentTimeMillis());
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    System.out.println("IntTestHarness - ConsumerConfig: Topic:" + topic + " properties:" + consumerConfig);

    try (KafkaConsumer<K, V> consumer = new KafkaConsumer(consumerConfig,
            keyDeserializer,
            valueDeserializer)) {

      consumer.subscribe(Collections.singleton(topic));

      Map<K, V> result = new HashMap<>();

      int waitCount = 0;
      while (result.size() < expectedNumMessages && waitCount++ < 5) {
        for (ConsumerRecord<K, V> record : consumer.poll(resultsPollMaxTimeMs)) {
          if (record.value() != null) {
            result.put(record.key(), record.value());
          }
        }
      }

      return result;
    }
  }

  private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
    return maxMessages < 0 || messagesConsumed < maxMessages;
  }


}
