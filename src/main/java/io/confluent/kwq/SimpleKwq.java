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

import io.confluent.ksql.util.KsqlConfig;
import io.confluent.kwq.util.KafkaTopicClient;
import io.confluent.kwq.util.KafkaTopicClientImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private final int numPartitions;
  private final short replicationFactor;
  private final Properties consumerConfig;
  private final List<String> topics = new ArrayList<>();
  private final List<KafkaConsumer> consumers = new ArrayList<>();

  private final KafkaTopicClient topicClient;

  public SimpleKwq(int numPriorities, String prefix, String bootstrapServers, int numPartitions, short replicationFactor) {
    this.numPriorities = numPriorities;
    this.prefix = prefix.toUpperCase();
    this.numPartitions = numPartitions;
    this.replicationFactor = replicationFactor;

    consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, prefix);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

    KsqlConfig ksqlConfig = new KsqlConfig(consumerConfig);

    Map<String, Object> ksqlAdminClientConfigProps = ksqlConfig.getKsqlAdminClientConfigProps();

    AdminClient adminClient = AdminClient.create(ksqlAdminClientConfigProps);
    this.topicClient = new KafkaTopicClientImpl(adminClient);
  }


  public void start() {
    createPriorityTopics();
  }

  private void createPriorityTopics() {

    int count = 0;
    for (int priority = 1; priority <= numPriorities; priority++) {
      String topicName = prefix + "-" + priority;
      topicClient.createTopic(topicName, numPartitions, replicationFactor);
      topics.add(topicName);

      Properties config = new Properties();
      config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
      config.put(ConsumerConfig.GROUP_ID_CONFIG, prefix);
      config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

  public Task consume() {

    while (nextRecords == null || !nextRecords.iterator.hasNext()) {
      nextRecords = getRecordsFromHighestPriorityTopic();
      if (nextRecords == null || nextRecords.iterator.hasNext()) {
        try {
          Thread.sleep(IDLE_WAIT_MS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    return nextRecords.iterator.next().value();
  }

  @Override
  public void pause() {

  }

  @Override
  public String status() {
    return "running... yay";

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
}
