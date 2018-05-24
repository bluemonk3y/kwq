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
import io.confluent.kwq.util.LockfreeConcurrentQueue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;


public class TaskStatsCollector {
  private static final Logger log = LoggerFactory.getLogger(TaskStatsCollector.class);

  public static final int STATS_RETENTION = 2000;
  private final Topology topology;
  private TaskStats currentWindowStats;
  private final StreamsConfig streamsConfig;
  private KafkaStreams streams;
  private final Queue<TaskStats> stats = new LockfreeConcurrentQueue<>();
  private int windowDurationS;

  public TaskStatsCollector(final String taskStatusTopic, final StreamsConfig streamsConfig, final int windowDurationS){
    this.streamsConfig = streamsConfig;
    this.topology = buildTopology(taskStatusTopic, windowDurationS);
  }

  private Topology buildTopology(final String taskStatusTopic, final int windowDurationS) {
    this.windowDurationS = windowDurationS;
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Task> tasks = builder.stream(taskStatusTopic);

    KTable<Windowed<String>, TaskStats> windowedTaskStatsKTable = tasks
            .groupBy((key, value) -> "agg-all-values")
            .windowedBy(TimeWindows.of(windowDurationS * 1000))
            .aggregate(
                    TaskStats::new,
                    (key, value, aggregate) -> aggregate.add(value),
                    Materialized.with(new Serdes.StringSerde(), new TaskStats.TaskStatsSerde())
            );

    /**
     * We only want to view the final value of each window, and not every CDC event, so use a window threshold.
     */
    windowedTaskStatsKTable.toStream().foreach( (key, value) -> {
              log.debug("Processing:{} time:{}", value, key.window().end());
              if (currentWindowStats != null && key.window().end() != currentWindowStats.getTime()) {
                log.debug("Adding:{} time:{}", currentWindowStats, key.window().end());
                stats.add(currentWindowStats);
                if (stats.size() > STATS_RETENTION) {
                  stats.remove();
                }
                // TODO: Publish stats onto a Topic for visualization via Grafana (store in elastic or influx)
              }
              currentWindowStats = value;
              currentWindowStats.setTime(key.window().end());
            }
    );
    return builder.build();
  }

  public void start() {
    streams = new KafkaStreams(topology, streamsConfig);
    streams.start();
  }
  public void stop() {
    streams.close();
  }

  public List<TaskStats> getStats() {
    if (currentWindowStats != null && currentWindowStats.getTime() < System.currentTimeMillis() - (windowDurationS * 1000)) {
      stats.add(currentWindowStats);
      currentWindowStats = null;
    } else if (currentWindowStats == null) {
      currentWindowStats = new TaskStats();
      currentWindowStats.setTime(System.currentTimeMillis() - (windowDurationS * 1000));
    }
    CopyOnWriteArrayList results = new CopyOnWriteArrayList<>(stats);
    if (currentWindowStats != null) results.add(currentWindowStats);
    Collections.reverse(results);
    return results;
  }

  public Topology getTopology() {
    return topology;
  }

}
