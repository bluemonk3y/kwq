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

import java.util.Date;

public class TaskStats {

  private int total;
  private int submitted;
  private int allocated;
  private int running;
  private int error;
  private int completed;

  private long time;

  public TaskStats add(Task task) {
    total++;
    if (task.getStatus() == Task.Status.SUBMITTED) submitted++;
    if (task.getStatus() == Task.Status.ALLOCATED) allocated++;
    if (task.getStatus() == Task.Status.RUNNING) running++;
    if (task.getStatus() == Task.Status.COMPLETED) completed++;
    if (task.getStatus() == Task.Status.ERROR) error++;
    return this;

  }

  @Override
  public String toString() {
    return "TaskStats{" +
            "total=" + total +
            ", submitted=" + submitted +
            ", running=" + running +
            ", error=" + error +
            ", completed=" + completed +
            ", time=" + new Date(time) +
            '}';
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

  public int getSubmitted() {
    return submitted;
  }

  public void setSubmitted(int submitted) {
    this.submitted = submitted;
  }

  public int getAllocated() {
    return allocated;
  }

  public void setAllocated(int allocated) {
    this.allocated = allocated;
  }

  public int getRunning() {
    return running;
  }

  public void setRunning(int running) {
    this.running = running;
  }

  public int getError() {
    return error;
  }

  public void setError(int error) {
    this.error = error;
  }

  public int getCompleted() {
    return completed;
  }

  public void setCompleted(int completed) {
    this.completed = completed;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  static public final class TaskStatsSerde extends WrapperSerde<TaskStats> {
    public TaskStatsSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(TaskStats.class));
    }
  }

}
