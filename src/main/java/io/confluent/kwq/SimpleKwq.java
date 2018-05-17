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
package io.confluent.kwq;

import io.confluent.kwq.util.KafkaTopicClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * SimpleKwq uses the requesting thread to service the next ConsumerAndRecords[] set iterator.
 * Note: This impl relies on auto-commit; as a result - if killed, it can potentially lose messages that were held in-memory, but not yet dispatched.
 */
public class SimpleKwq implements Kwq {
  private static final int CONSUMER_POLL_TIMEOUT_MS = 100;
  private static final long IDLE_WAIT_MS = 1000L;

  private final int numPriorities;
  private final String prefix;
  private String bootstrapServers;
  private final int numPartitions;
  private final short replicationFactor;
  private final List<String> topics = new ArrayList<>();
  private final List<KafkaConsumer> consumers = new ArrayList<>();
  private KafkaProducer producer = null;

  private final KafkaTopicClient topicClient;

  public SimpleKwq(KafkaTopicClient topicClient, int numPriorities, String prefix, String bootstrapServers, int numPartitions, short replicationFactor) {
    this.numPriorities = numPriorities;
    this.prefix = prefix.toUpperCase();
    this.bootstrapServers = bootstrapServers;
    this.numPartitions = numPartitions;
    this.replicationFactor = replicationFactor;
    this.topicClient = topicClient;
  }


  public void start() {
    createPriorityTopics();
  }

  private void createPriorityTopics() {

    producer = new KafkaProducer<>(producerConfig(bootstrapServers), new StringSerializer(), new TaskSerDes());

    int count = 0;
    for (int priority = 1; priority <= numPriorities; priority++) {
      String topicName = prefix + "-" + priority;
      topicClient.createTopic(topicName, numPartitions, replicationFactor);
      topics.add(topicName);

      Properties config = consumerConfig(bootstrapServers);

      config.put(ConsumerConfig.CLIENT_ID_CONFIG, topicName + "-" + count++);
      config.put(ConsumerConfig.GROUP_ID_CONFIG, topicName + "-" + count++);

      KafkaConsumer<String, Task> consumer = new KafkaConsumer(config,
              new StringDeserializer(),
              new TaskSerDes());

      consumer.subscribe(Collections.singleton(topicName));
      consumers.add(consumer);
    }
  }

  private ConsumerAndRecords nextRecords;

  synchronized public Task  consume() {
    try {
    while (nextRecords == null || !nextRecords.iterator.hasNext()) {
      nextRecords = getRecordsFromHighestPriorityTopic();
      if (nextRecords == null || nextRecords.iterator.hasNext()) {

          Thread.sleep(IDLE_WAIT_MS);
      }
    }
    return nextRecords.iterator.next().value();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void pause() {
  }

  @Override
  public String status() {
    return "running... yay";
  }

  @Override
  synchronized public void submit(Task task) {

    // Note: acks = all = means all replica's have received and acknowledged all events - this is slow but accurate...
    int priority = task.getPriority();
    if (priority <= 1) priority = 1;
    if (priority > topics.size()) priority = topics.size();
    producer.send(new ProducerRecord<>(topics.get(priority), task.getId(), task));
    producer.flush();
  }

  private ConsumerAndRecords getRecordsFromHighestPriorityTopic() {
    ConsumerRecords<String,Task> results;
    for (int i = consumers.size()-1; i >= 0; i--) {
      KafkaConsumer<String, Task> consumer = consumers.get(i);
      results = consumer.poll(CONSUMER_POLL_TIMEOUT_MS);
      if (!results.isEmpty()) {
        return new ConsumerAndRecords(consumer, results.iterator());
      }
    }
    return null;
  }

  public static class ConsumerAndRecords {
    final KafkaConsumer consumer;
    final Iterator<ConsumerRecord<String, Task>> iterator;

    public ConsumerAndRecords(KafkaConsumer<String, Task> consumer, Iterator<ConsumerRecord<String, Task>> iterator) {
      this.consumer = consumer;
      this.iterator = iterator;
    }
  }

  private Properties consumerConfig(String bootstrapServers) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, prefix);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return config;
  }

  private Properties producerConfig(String bootstrapServers) {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    return producerConfig;
  }

}
