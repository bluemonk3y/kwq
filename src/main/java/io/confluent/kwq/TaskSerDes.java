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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class TaskSerDes implements Serde<Task>, Serializer<Task>, Deserializer<Task> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  public Task deserialize(String s, byte[] bytes) {
    try {
      if (bytes == null) {
        throw new RuntimeException(this.getClass().getSimpleName() + ": Cannot read 'null' record for key:" + s);
      }
      JsonNode jsonNode = this.objectMapper.readTree(bytes);

      return new Task(
              jsonNode.get("id").asText(),
              jsonNode.get("groupId").asText(),
              jsonNode.get("priority").asInt(),
              jsonNode.get("tag").asText(),
              jsonNode.get("source").asText(),
              jsonNode.get("payload").asText(),
              Task.Status.valueOf(jsonNode.get("status").asText()),
              jsonNode.get("worker").asText(),
              jsonNode.get("workerEndpoint").asText(),
              jsonNode.get("runCount").asInt(),
              jsonNode.get("meta").asText(),
              jsonNode.get("timeoutSeconds").asInt(),
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

  @Override
  public Serializer<Task> serializer() {
    return this;
  }

  @Override
  public Deserializer<Task> deserializer() {
    return this;
  }
}
