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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class Simulator {
  static Logger log = LoggerFactory.getLogger(Simulator.class);
  private Kwq kwq;
  private TaskStatus taskStatus;

  public Simulator(Kwq kwq, TaskStatus taskStatus) {
    this.kwq = kwq;
    this.taskStatus = taskStatus;
  }

  public int simulate(String groupId, int numberOfTasks, int durationSeconds, int numberOfWorkers) {

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    log.info("Running simulation:" + groupId + " Tasks:" + numberOfTasks + " duration:" + durationSeconds + " workers:" + numberOfWorkers);
    ScheduledExecutorService workerRunPool = newScheduledThreadPool(numberOfWorkers);
    ExecutorService workerRequestPool = newFixedThreadPool(numberOfWorkers);
    String sourceTag = "tag-" + SimpleDateFormat.getDateInstance().format(new Date());
    final AtomicInteger completeCount = new AtomicInteger();
    final AtomicInteger acceptCount = new AtomicInteger();
    final AtomicInteger waitCount = new AtomicInteger();
    for (int i = 0; i < numberOfTasks; i++) {
      Task task = new Task(i + "-" + groupId, groupId, (i % 5)+1, "tag-" + groupId, "source-" + sourceTag, "payload", Task.Status.ALLOCATED, "worker", "worker-ep", 0, "meta", 60, System.currentTimeMillis(), System.currentTimeMillis(), 0, 0);
      log.info(" Submitting:" + task.getId() + " Task:" + i + " of " + numberOfTasks);
      kwq.submit(task);
      task.setStatus(Task.Status.ALLOCATED);
      taskStatus.update(task);
    }

    log.info("=========Consuming tasks ");
    for (int i = 0; i < numberOfTasks; i++) {
      workerRequestPool.submit(() -> simulateWorker(durationSeconds, workerRunPool, completeCount, acceptCount, waitCount));
    }

    workerRequestPool.shutdown();
    try {
      workerRequestPool.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return completeCount.get();
  }

  public void simulateWorker(int durationSeconds, ScheduledExecutorService scheduler, AtomicInteger completeCount, AtomicInteger acceptCount, AtomicInteger waitCount) {
    log.info(" Worker wait:{}", waitCount.incrementAndGet());

    Task task = kwq.consume();
    task.setStatus(Task.Status.RUNNING);
    taskStatus.update(task);


    log.info(" Worker consumed Task:{} Priority:{} #Accept:{}", task.getId(), task.getPriority(), acceptCount.incrementAndGet());

    // simulate the task duration using the scheduler
    scheduler.schedule(() -> {
      log.info(" Completed:{} Count:{} Priority:{}", task.getId(), completeCount.incrementAndGet(), task.getPriority());
      task.setStatus(Task.Status.COMPLETED);
      taskStatus.update(task);
    }, durationSeconds, TimeUnit.SECONDS);
  }
}
