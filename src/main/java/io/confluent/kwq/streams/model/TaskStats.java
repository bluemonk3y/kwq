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
package io.confluent.kwq.streams.model;

import io.confluent.kwq.Task;

public class TaskStats {

  private long total;
  private long running;
  private long error;
  private long completed;

  public long getTotal() {
    return total;
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

  public TaskStats add(Task task) {
    total++;
    if (task.getStatus() == Task.Status.RUNNING) running++;
    if (task.getStatus() == Task.Status.COMPLETED) completed++;
    if (task.getStatus() == Task.Status.ERROR) error++;
    return this;

  }

  static public final class TaskStatsSerde extends WrapperSerde<TaskStats> {
    public TaskStatsSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(TaskStats.class));
    }
  }

}
