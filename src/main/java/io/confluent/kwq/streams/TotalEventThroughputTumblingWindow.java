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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;


public class TotalEventThroughputTumblingWindow {

  private final Topology topology;
  private int windowDurationS = 5;
  private long totalEvents;
  private long currentEvents;
  private final StreamsConfig streamsConfig;
  private long running;
  private long error;
  private long completed;

  public TotalEventThroughputTumblingWindow(String taskStatusTopic, StreamsConfig streamsConfig, int windowDurationS){
    this.streamsConfig = streamsConfig;
    this.windowDurationS = windowDurationS;
    this.topology = buildTopology(taskStatusTopic);
  }

  private Topology buildTopology(String taskStatusTopic) {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Task> tasks = builder.stream(taskStatusTopic);


    KTable<Windowed<String>, Long> count = tasks.groupBy((key, value) -> "agg-all-values").
            windowedBy(TimeWindows.of(windowDurationS * 1000)).count();

    /**
     * We only want to view the final value of each window, and not every CDC event, so use a window threshold.
     * Note: this is problematic when data stops and we could potentially use a 'future' to manage missing adjacent window - we really need a callback on window expiry
     */
    count.toStream().foreach(new ForeachAction<Windowed<String>, Long>() {
      public long lastWindowStart = -1;
      public long lastValue;

      @Override
      public void apply(Windowed<String> key, Long value) {
        if (key.window().start() != lastWindowStart) {
          lastWindowStart = key.window().start();
          // publish the last counted value
          totalEvents = lastValue;
        }
        lastValue = value;
        currentEvents = value;
      }
    });

    return builder.build();
  }

  public void start() {
    KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();
  }

  public long getTotalEvents() {
    return totalEvents;
  }
  public long getCurrentEvents() {
    return currentEvents;
  }

  public Topology getTopology() {
    return topology;
  }

  public long getRunning() {
    return running;
  }

  public long getError() {
    return error;
  }

  public long getCompleted() {
    return completed;
  }
}
