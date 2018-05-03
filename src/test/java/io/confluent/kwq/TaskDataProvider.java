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

import java.util.Map;
import java.util.TreeMap;

public class TaskDataProvider  {

  public static final Map<String, Task> data = new TaskDataProvider().buildData();

  private Map<String, Task> buildData() {
    Map<String, Task> dataMap = new TreeMap<>();
    dataMap.put("0", new Task("0", "job1", 1, "tag-1", Task.Status.SUBMITTED, "PAYLOAD-0", 0, 0, 0, 0));
    dataMap.put("1", new Task("1", "job1", 2, "tag-2", Task.Status.SUBMITTED, "PAYLOAD-1", 0, 0, 0, 0));
    dataMap.put("2", new Task("2", "job2", 1, "tag-1", Task.Status.ALLOCATED, "PAYLOAD-2", 0, 0, 0, 0));
    dataMap.put("3", new Task("3", "job2", 2, "tag-2", Task.Status.RUNNING, "PAYLOAD-3", 0, 0, 0, 0));
    dataMap.put("4", new Task("4", "job3", 1, "tag-1", Task.Status.COMPLETED, "PAYLOAD-4", 0, 0, 0, 0));
    dataMap.put("5", new Task("5", "job4", 1, "tag-1", Task.Status.COMPLETED, "PAYLOAD-5", 0, 0, 0, 0));
    return dataMap;
  }


}