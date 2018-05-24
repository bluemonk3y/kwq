package io.confluent.kwq;

import io.confluent.kwq.streams.TaskStatsCollector;
import io.confluent.kwq.streams.model.TaskStats;
import io.confluent.kwq.util.KafkaTopicClient;
import io.confluent.kwq.util.LockfreeConcurrentQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;

public class TaskStatusImpl implements TaskStatus {

  private static final String TASK_STATUS_TOPIC = "kwqTaskStatus";
  public static final int MAX_TRACKING_TASK_COUNT = Integer.getInteger("task.history.size", 5000);
  private final KafkaProducer<String, Task> producer;
  private KafkaTopicClient topicClient;
  private final Queue<Task> recentTasks = new LockfreeConcurrentQueue<>();
  private TaskStatsCollector taskStatsCollector;

  public TaskStatusImpl(String bootstrapServers, KafkaTopicClient topicClient, int partitions, short replicationFactor){
    producer = new KafkaProducer<>(producerProperties(bootstrapServers), new StringSerializer(), new TaskSerDes());
    this.topicClient = topicClient;
    startStreamsJobs(bootstrapServers, partitions, replicationFactor);
  }

  private void startStreamsJobs(String bootstrapServers, int partitions, short replicationFactor) {

    topicClient.createTopic(TASK_STATUS_TOPIC, partitions, replicationFactor);
    taskStatsCollector = new TaskStatsCollector(TASK_STATUS_TOPIC, streamsProperties(bootstrapServers, "total-events"), 5);
    taskStatsCollector.start();
  }


  @Override
  public void update(Task task) {
    producer.send(new ProducerRecord<>(TASK_STATUS_TOPIC, task.getId(), task));
    manageTaskHistoryQueue(task);
  }

  public void manageTaskHistoryQueue(Task task) {
    recentTasks.add(task);
    // Note: we use an recentTasksSize because recentTasks.size() iterates the collection
    if (recentTasks.size() > MAX_TRACKING_TASK_COUNT) {
      recentTasks.remove();
    }
  }

  @Override
  public List<Task> tasks() {
    return new CopyOnWriteArrayList<>(recentTasks);
  }

  @Override
  public List<TaskStats> getStats() {
    return taskStatsCollector.getStats();
  }

  private Properties producerProperties(String bootstrapServers) {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    return producerConfig;
  }

  private StreamsConfig streamsProperties(String bootstrapServers, String applicationId){
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TaskSerDes.class);
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
    return new StreamsConfig(config);
  }
}
