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
import io.confluent.kwq.streams.model.TaskStats;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;


public class TaskStatsCollector {

  private final Topology topology;
  private int windowDurationS = 5;
  private TaskStats lastWindowStats;
  private TaskStats currentWindowStats;
  private final StreamsConfig streamsConfig;
  private long lastWindowStart = -1;

  public TaskStatsCollector(String taskStatusTopic, StreamsConfig streamsConfig, int windowDurationS){
    this.streamsConfig = streamsConfig;
    this.windowDurationS = windowDurationS;
    this.topology = buildTopology(taskStatusTopic);
  }

  private Topology buildTopology(String taskStatusTopic) {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Task> tasks = builder.stream(taskStatusTopic);


    KTable<Windowed<String>, TaskStats> windowedTaskStatsKTable = tasks
            .groupBy((key, value) -> "agg-all-values")
            .aggregate(
                    TaskStats::new,
                    (String key, Task value, TaskStats aggregate) -> aggregate.add(value),
                    TimeWindows.of(windowDurationS * 1000),
                    new TaskStats.TaskStatsSerde(),
                    "task-stats-store");


    /**
     * We only want to view the final value of each window, and not every CDC event, so use a window threshold.
     * Note: this is problematic when data stops and we could potentially use a 'future' to manage missing adjacent window - we really need a callback on window expiry
     */
    windowedTaskStatsKTable.toStream().foreach( (key, value) -> {
      if (key.window().start() != lastWindowStart) {
          lastWindowStart = key.window().start();
          // publish the last counted value
          lastWindowStats = currentWindowStats;
//          TODO: Publish stats onto Topic for visualization via Grafana (store in elastic or influx)
        }
        currentWindowStats = value;
      }
    );

    return builder.build();
  }

  public void start() {
    KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();
  }

  public TaskStats getLastWindowStats() {
    return lastWindowStats;
  }
  public TaskStats getCurrentStats() {
    return currentWindowStats;
  }

  public Topology getTopology() {
    return topology;
  }

}
