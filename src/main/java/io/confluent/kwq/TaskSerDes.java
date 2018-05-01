package io.confluent.kwq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class TaskSerDes implements Serializer<Task>, Deserializer<Task> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Task deserialize(String s, byte[] bytes) {
    try {
      if (bytes == null) {
        throw new RuntimeException(this.getClass().getSimpleName() + ": Cannot read 'null' record for key:" + s);
      }
      JsonNode jsonNode = this.objectMapper.readTree(bytes);

      return new Task(
              jsonNode.get("id").asText(),
              jsonNode.get("priority").asInt(),
              jsonNode.get("tag").asText(),
              Task.Status.valueOf(jsonNode.get("status").asText()),
              jsonNode.get("payload").asText(),
              jsonNode.get("submitted_ts").asLong(),
              jsonNode.get("allocated_ts").asLong(),
              jsonNode.get("running_ts").asLong(),
              jsonNode.get("completed_ts").asLong()
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String s, Task task) {
    try {
      return this.objectMapper.writeValueAsBytes(task);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {

  }
}
