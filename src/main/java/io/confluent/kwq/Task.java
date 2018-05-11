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

public class Task implements Comparable<Task> {


  public void setId(String id) {
    this.id = id;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public void setStatus(Status status) {
    if (status == Status.ALLOCATED) allocated_ts = System.currentTimeMillis();
    if (status == Status.SUBMITTED) submitted_ts = System.currentTimeMillis();
    if (status == Status.RUNNING) running_ts = System.currentTimeMillis();
    if (status == Status.COMPLETED) completed_ts = System.currentTimeMillis();
    this.status = status;
  }

  public void setWorker(String worker) {
    this.worker = worker;
  }

  public void setRunCount(int runCount) {
    this.runCount = runCount;
  }

  public void setWorkerEndpoint(String workerEndpoint) {
    this.workerEndpoint = workerEndpoint;
  }

  public void setMeta(String meta) {
    this.meta = meta;
  }

  public void setTimeoutSeconds(long timeoutSeconds) {
    this.timeoutSeconds = timeoutSeconds;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public void setSubmitted_ts(long submitted_ts) {
    this.submitted_ts = submitted_ts;
  }

  public void setAllocated_ts(long allocated_ts) {
    this.allocated_ts = allocated_ts;
  }

  public void setRunning_ts(long running_ts) {
    this.running_ts = running_ts;
  }

  public void setCompleted_ts(long completed_ts) {
    this.completed_ts = completed_ts;
  }

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
          String source,
          String payload,
          Status status,
          String worker,
          String workerEndpoint,
          int runCount,
          String meta,
          long timeoutSeconds,
          long submitted_ts,
          long allocated_ts,
          long running_ts,
          long completed_ts) {

    this.id = id;
    this.groupId = groupId;
    this.priority = priority;
    this.tag = tag;
    this.source = source;
    this.payload = payload;
    this.status = status;
    this.worker = worker;
    this.runCount = runCount;
    this.workerEndpoint = workerEndpoint;
    this.meta = meta;
    this.timeoutSeconds = timeoutSeconds;
    this.submitted_ts = submitted_ts;
    this.allocated_ts = allocated_ts;
    this.running_ts = running_ts;
    this.completed_ts = completed_ts;
  }

  private String id;
  private String groupId;
  private int priority;
  private String tag;
  private String source;
  private Status status;
  private String worker;
  private int runCount;
  private String workerEndpoint;
  private String meta;
  private long timeoutSeconds;
  private String payload;
  private long submitted_ts;
  private long allocated_ts;
  private long running_ts;
  private long completed_ts;

  public String getId() {
    return id;
  }

  public String getGroupId() {
    return groupId;
  }

  public int getPriority() {
    return priority;
  }

  public String getTag() {
    return tag;
  }

  public String getSource() {
    return source;
  }

  public Status getStatus() {
    return status;
  }

  public String getWorker() {
    return worker;
  }

  public int getRunCount() {
    return runCount;
  }

  public long getTimeoutSeconds() {
    return timeoutSeconds;
  }

  public String getWorkerEndpoint() {
    return workerEndpoint;
  }

  public String getMeta() {
    return meta;
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

  @Override
  public String toString() {
    return "Task{" +
            "id='" + id + '\'' +
            ", groupId='" + groupId + '\'' +
            ", priority=" + priority +
            ", tag='" + tag + '\'' +
            ", source='" + source + '\'' +
            ", status=" + status +
            ", worker='" + worker + '\'' +
            ", workerEndpoint='" + workerEndpoint + '\'' +
            ", meta=" + meta +
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

//  meh!
// public static class TaskBuilder {
//    final Map<String, Object> values = new HashMap<>();
//
//    TaskBuilder id(String value) {
//      return putInt("id", value);
//    }
//    TaskBuilder groupId(String value) {
//      return putInt("groupId", value);
//    }
//    TaskBuilder priority(int value) {
//      return putInt("priority", value);
//    }
//    TaskBuilder tag(String value) {
//      return putInt("tag", value);
//    }
//    TaskBuilder status(String value) {
//      return putInt("status", value);
//    }
//    TaskBuilder payload(String value) {
//      return putInt("payload", value);
//    }
//    TaskBuilder submitted_ts(long value) {
//      return putInt("submitted_ts", value);
//    }
//    TaskBuilder allocated_ts(long value) {
//      return putInt("allocated_ts", value);
//    }
//    TaskBuilder running_ts(long value) {
//      return putInt("running_ts", value);
//    }
//    TaskBuilder completed_ts(long value) {
//      return putInt("completed_ts", value);
//    }
//
//    public Task build() {
//      return new Task(
//              get("id"), get("groupId"), (int) values.get("priority"), get("tag"), Status.valueOf(get("status")), get("payload"),
//              getLong("submitted_ts"), getLong("allocated_ts"), getLong("running_ts"), getLong("completed_ts")
//      );
//    }
//
//    private long getLong(String key) {
//      return (long) values.getOrDefault(key, 0L);
//    }
//
//    private String get(String key) {
//      return (String) values.get(key);
//    }
//
//    private TaskBuilder putInt(String key, Object value) {
//      values.put(key, value);
//      return this;
//    }
//  }
}