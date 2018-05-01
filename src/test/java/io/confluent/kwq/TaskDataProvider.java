package io.confluent.kwq;

import scala.collection.generic.Sorted;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TaskDataProvider  {

  enum Status { SUBMITTED, ALLOCATED, RUNNING, COMPLETED};

  public static final Map<String, Task> data = new TaskDataProvider().buildData();

  private Map<String, Task> buildData() {
    Map<String, Task> dataMap = new TreeMap<>();
    dataMap.put("0", new Task("0", 1, "tag-1", Task.Status.SUBMITTED, "PAYLOAD-0", 0l, 0l, 0l, 0l));
    dataMap.put("1", new Task("1", 2, "tag-2", Task.Status.SUBMITTED, "PAYLOAD-1", 0l, 0l, 0l, 0l));
    dataMap.put("2", new Task("2", 1, "tag-1", Task.Status.ALLOCATED, "PAYLOAD-2", 0l, 0l, 0l, 0l));
    dataMap.put("3", new Task("3", 2, "tag-2", Task.Status.RUNNING, "PAYLOAD-3", 0l, 0l, 0l, 0l));
    dataMap.put("4", new Task("4", 1, "tag-1", Task.Status.COMPLETED, "PAYLOAD-4", 0l, 0l, 0l, 0l));
    dataMap.put("5", new Task("5", 1, "tag-1", Task.Status.COMPLETED, "PAYLOAD-5", 0l, 0l, 0l, 0l));
    return dataMap;
  }


}