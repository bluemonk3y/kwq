package io.confluent.kwq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;

public class AckingSimpleKwq extends SimpleKwq {

  public AckingSimpleKwq(int numPriorities, String prefix, String boostrap_servers, int numPartitions, short replicationFactor) {
    super(numPriorities, prefix, boostrap_servers, numPartitions, replicationFactor);
  }

  @Override
  public Task consume() {

      while (nextRecords == null || !nextRecords.iterator.hasNext()) {
        nextRecords = getRecords();
        if (nextRecords == null || nextRecords.iterator.hasNext()) {
          try {
            Thread.sleep(IDLE_WAIT_MS);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    ConsumerRecord<String, Task> next = nextRecords.iterator.next();
    nextRecords.consumer.commitSync(Collections.singletonMap(new TopicPartition(next.topic(), next.partition()), new OffsetAndMetadata(next.offset())));
    return next.value();
  }
}
