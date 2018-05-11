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

import java.util.Properties;

public class KwqInstance {

  private final Kwq kwq;
  private final TaskStatus taskStatus;

  public KwqInstance(Kwq kwq, TaskStatus taskStatus) {
    this.kwq = kwq;
    this.taskStatus = taskStatus;
  }

  public Kwq getKwq() {
    return kwq;
  }



  /**
   * Note: dont care about double locking because it is always created on startup in the Servlet Lifecycle.start()
   */
  static KwqInstance singleton = null;
  public static KwqInstance getInstance(Properties propertes) {
    if (singleton == null) {
      SimpleKwq kwq = new SimpleKwq(
              Integer.valueOf(propertes.getProperty("numPriorities", "9")),
              propertes.getProperty("prefix", "kwq"),
              propertes.getProperty("bootstrap.servers", "localhost:9092"),
              Integer.valueOf(propertes.getProperty("numPartitions", "3")),
              Short.valueOf(propertes.getProperty("numReplicas", "1"))
      );
      kwq.start();
      singleton = new KwqInstance(
              kwq,
                new TaskStatusImpl(propertes.getProperty("bootstrap.servers", "localhost:9092"))
        );
        return singleton;
    }
    return singleton;
  }

  public TaskStatus getTaskStatus() {
    return taskStatus;
  }
}
