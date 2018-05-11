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

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class Simulator {
  private Kwq kwq;
  private TaskStatus taskStatus;

  public Simulator(Kwq kwq, TaskStatus taskStatus) {
    this.kwq = kwq;
    this.taskStatus = taskStatus;
  }

  public int simulate(String groupId, int numberOfTasks, int durationSeconds, int numberOfWorkers) {

    System.out.println("Running simulation:" + groupId);
    ScheduledExecutorService scheduler = newScheduledThreadPool(5);
    ExecutorService fixedThreadPool = newFixedThreadPool(numberOfWorkers);
    String sourceTag = new Date().toString();
    for (int i = 0; i < numberOfTasks; i++) {
      Task task = new Task(i + "-" + sourceTag, groupId, (i % 5)+1, "tag-" + groupId, "source-" + sourceTag, "payload", Task.Status.ALLOCATED, "worker", "worker-ep", 0, "meta", 60, System.currentTimeMillis(), System.currentTimeMillis(), 0, 0);
      System.out.println(Thread.currentThread().getName() + " Submitting:" + task.getId());
      kwq.submit(task);
      taskStatus.update(task);
      fixedThreadPool.submit(() -> simulateWorker(durationSeconds, scheduler));
    }
    return 0;
  }

  public void simulateWorker(int durationSeconds, ScheduledExecutorService scheduler) {
    ScheduledFuture<Task> taskScheduledFuture = scheduler.schedule(() -> kwq.consume(), 1, TimeUnit.MILLISECONDS);

    try {
      System.out.println("Worker requesting task:");
      Task updatedTask = taskScheduledFuture.get();
      System.out.println("Worker Consumed:" + updatedTask.getId());

      updatedTask.setStatus(Task.Status.COMPLETED);
      scheduler.schedule(() -> {
        System.out.println("Completed:" + updatedTask.getId());
        kwq.submit(updatedTask);
      }, durationSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }
}
