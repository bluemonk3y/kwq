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
package io.confluent.kwq.streams;

import io.confluent.kwq.Task;
import io.confluent.kwq.TaskDataProvider;
import io.confluent.kwq.TaskSerDes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class TotalEventsTumblingWindowTest {



  @Test
  public void getTotalWindowEvents() throws Exception {

    StreamsConfig streamsConfig = new StreamsConfig(getProperties());

    TotalEventThroughputTumblingWindow totalEvents = new TotalEventThroughputTumblingWindow("TestTopic", streamsConfig, 1);

    Topology topology = totalEvents.getTopology();

    ProcessorTopologyTestDriver driver = new ProcessorTopologyTestDriver(streamsConfig, topology);

    Map<String, Task> data = TaskDataProvider.data;
    Task task = data.values().iterator().next();

    for (int i = 0; i < 10; i++) {
      driver.process("TestTopic", "task", task, Serdes.String().serializer(), new TaskSerDes(), 1000);
    }
    driver.process("TestTopic", "task", task, Serdes.String().serializer(), new TaskSerDes(), 2000);

    driver.close();
    // read the current throughput -= should be 1
    Assert.assertEquals(10, totalEvents.getTotalEvents());
  }


  @Test
  public void getCurrentEvents() throws Exception {

    StreamsConfig streamsConfig = new StreamsConfig(getProperties());

    TotalEventThroughputTumblingWindow totalEvents = new TotalEventThroughputTumblingWindow("TestTopic", streamsConfig, 1);

    Topology topology = totalEvents.getTopology();

    ProcessorTopologyTestDriver driver = new ProcessorTopologyTestDriver(streamsConfig, topology);

    Map<String, Task> data = TaskDataProvider.data;

    driver.process("TestTopic", "task", data.values().iterator().next(), Serdes.String().serializer(), new TaskSerDes());

    driver.close();
    // read the current throughput -= should be 1
    Assert.assertEquals(1, totalEvents.getCurrentEvents());
  }

  private Properties getProperties() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + System.currentTimeMillis());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TaskSerDes.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

}