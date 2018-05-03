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

import java.util.HashMap;
import java.util.Map;

public class Task implements Comparable<Task> {


  public enum Status {
    SUBMITTED, ALLOCATED, RUNNING, COMPLETED, ERROR
  }

  public Task() {
  }

  public Task(
          String id,
          String groupId,
          int priority,
          String tag,
          Status status,
          String payload,
          long submitted_ts,
          long allocated_ts,
          long running_ts,
          long completed_ts) {

    this.id = id;
    this.groupId = groupId;
    this.priority = priority;
    this.tag = tag;
    this.status = status;
    this.payload = payload;
    this.submitted_ts = submitted_ts;
    this.allocated_ts = allocated_ts;
    this.running_ts = running_ts;
    this.completed_ts = completed_ts;
  }

  private String id;
  private String groupId;
  private int priority;
  private String tag;
  private Status status;
  private String payload;
  private long submitted_ts;
  private long allocated_ts;
  private long running_ts;
  private long completed_ts;

  public int getPriority() {
    return priority;
  }

  public String getTag() {
    return tag;
  }

  public Status getStatus() {
    return status;
  }

  public String getPayload() {
    return payload;
  }

  public long getSubmitted_ts() {
    return submitted_ts;
  }

  public long getAllocated_ts() {
    return allocated_ts;
  }

  public long getRunning_ts() {
    return running_ts;
  }

  public long getCompleted_ts() {
    return completed_ts;
  }

  public String getId() {
    return id;
  }

  public String getGroupId() {
    return groupId;
  }

  @Override
  public String toString() {
    return "Task{" +
            "id='" + id + '\'' +
            "groupId='" + groupId + '\'' +
            ", priority=" + priority +
            ", tag='" + tag + '\'' +
            ", status='" + status + '\'' +
            ", payload='" + payload + '\'' +
            ", submitted_ts=" + submitted_ts +
            ", allocated_ts=" + allocated_ts +
            ", running_ts=" + running_ts +
            ", completed_ts=" + completed_ts +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Task task = (Task) o;
    return id.equals(task.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public int compareTo(Task o) {
    return Integer.compare(this.priority, o.priority);
  }

  public static class TaskBuilder {
    final Map<String, Object> values = new HashMap<>();

    TaskBuilder id(String value) {
      return putInt("id", value);
    }
    TaskBuilder groupId(String value) {
      return putInt("groupId", value);
    }
    TaskBuilder priority(int value) {
      return putInt("priority", value);
    }
    TaskBuilder tag(String value) {
      return putInt("tag", value);
    }
    TaskBuilder status(String value) {
      return putInt("status", value);
    }
    TaskBuilder payload(String value) {
      return putInt("payload", value);
    }
    TaskBuilder submitted_ts(long value) {
      return putInt("submitted_ts", value);
    }
    TaskBuilder allocated_ts(long value) {
      return putInt("allocated_ts", value);
    }
    TaskBuilder running_ts(long value) {
      return putInt("running_ts", value);
    }
    TaskBuilder completed_ts(long value) {
      return putInt("completed_ts", value);
    }

    public Task build() {
      return new Task(
              get("id"), get("groupId"), (int) values.get("priority"), get("tag"), Status.valueOf(get("status")), get("payload"),
              getLong("submitted_ts"), getLong("allocated_ts"), getLong("running_ts"), getLong("completed_ts")
      );
    }

    private long getLong(String key) {
      return (long) values.getOrDefault(key, 0L);
    }

    private String get(String key) {
      return (String) values.get(key);
    }

    private TaskBuilder putInt(String key, Object value) {
      values.put(key, value);
      return this;
    }
  }
}