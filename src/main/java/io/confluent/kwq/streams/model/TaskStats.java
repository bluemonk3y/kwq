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
      super(new JsonSerializer<TaskStats>(), new JsonDeserializer<TaskStats>(TaskStats.class));
    }
  }

}
