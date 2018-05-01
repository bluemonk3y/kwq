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
          int priority,
          String tag,
          Status status,
          String payload,
          long submitted_ts,
          long allocated_ts,
          long running_ts,
          long completed_ts) {

    this.id = id;
    this.priority = priority;
    this.tag = tag;
    this.status = status;
    this.payload = payload;
    this.submitted_ts = submitted_ts;
    this.allocated_ts = allocated_ts;
    this.running_ts = running_ts;
    this.completed_ts = completed_ts;
  }

  String id;
  int priority;
  String tag;
  Status status;
  String payload;
  long submitted_ts;
  long allocated_ts;
  long running_ts;
  long completed_ts;

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

  @Override
  public String toString() {
    return "Task{" +
            "id='" + id + '\'' +
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
    Map<String, Object> values = new HashMap<>();

    TaskBuilder id(String value) {
      return putInt("id", value);
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
              get("id"), (int) values.get("priority"), get("tag"), Status.valueOf(get("status")), get("payload"),
              getLong("submitted_ts"), getLong("allocated_ts"), getLong("running_ts"), getLong("completed_ts")
      );
    }

    private long getLong(String key) {
      return (long) values.getOrDefault(key, 0l);
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