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


import io.confluent.kwq.streams.model.TaskStats;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Date;

@Path("kwq")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class KwqRestEndpoint {

  private final KwqInstance kwqInstance;

  public KwqRestEndpoint(){
    kwqInstance = KwqInstance.getInstance(null);
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String sayPlainTextHello() {
    return "KwqRestEndpoint:" + kwqInstance.getKwq().toString() + " " + kwqInstance.getTaskStatus().toString();
  }

  @GET
  public String status() {
    return kwqInstance.getKwq().toString() + " " + kwqInstance.getTaskStatus().toString();
  }

  @GET
  @Path("consume/{workerAddress}/{workerEndpoint}/{waitSec}")
  @Operation(summary = "Allow a worker to consume the next available Task",
          tags = {"tasks"},
          responses = {
                  @ApiResponse(content = @Content(schema = @Schema(implementation = Task.class))),
                  @ApiResponse(responseCode = "400", description = "Invalid ID supplied"),
                  @ApiResponse(responseCode = "404", description = "Sim not found"),
                  @ApiResponse(responseCode = "405", description = "Invalid input")
          })
  public Task consume(
          @Parameter(description = "Location address of the worker requesting work", required = true)  @PathParam("workerAddress") String workerAddress,
          @Parameter(description = "Management address of the worker requesting work", required = true)@PathParam("workerEndpoint") String workerEndpoint,
          @Parameter(description = "Duration to wait for a task to become available", required = true) @PathParam("waitSec") long waitSec
  ) {
    Task allocatedTask = kwqInstance.getKwq().consume();
    allocatedTask.setStatus(Task.Status.ALLOCATED);
    allocatedTask.setWorker(workerAddress);
    allocatedTask.setWorkerEndpoint(workerEndpoint);
    kwqInstance.getTaskStatus().update(allocatedTask);

    // Note: should be updated by the worker when it has received the task (via Kafka Topic)
    allocatedTask.setStatus(Task.Status.RUNNING);
    kwqInstance.getTaskStatus().update(allocatedTask);
    return allocatedTask;
  }

  @POST
  @Path("submit")
  @Operation(summary = "Add a new Task to the Worker Queue",
          tags = {"tasks"},
          responses = {
                  @ApiResponse(content = @Content(schema = @Schema(implementation = Task.class))),
                  @ApiResponse(responseCode = "405", description = "Invalid input")
          })
  public Task submitTask(
          @Parameter(description = "Submit task to the worker queues", required = true) Task task) {
    kwqInstance.getKwq().submit(task);
    return task;
  }

  @POST
  @Path("updateTask")
  @Operation(summary = "Update the status of a Task",
          tags = {"tasks"},
          responses = {
                  @ApiResponse(content = @Content(schema = @Schema(implementation = Task.class))),
                  @ApiResponse(responseCode = "405", description = "Invalid input")
          })
  public Task updateTask(
          @Parameter(description = "Update the state of a tracked Task", required = true) Task task) {
    kwqInstance.getTaskStatus().update(task);
    return task;
  }

  @GET
  @Path("stats")
  @Operation(summary = "Return the current set of Task statistics",
          tags = {"tasks"},
          responses = {
                  @ApiResponse(content = @Content(schema = @Schema(implementation = TaskStats.class))),
                  @ApiResponse(responseCode = "405", description = "Invalid input")
          })
  public TaskStats getTaskStats() {
    return kwqInstance.getTaskStatus().getStats();
  }

  @GET
  @Path("simulate/{numberOfTasks}/{durationSeconds}/{numberOfWorkers}")
  @Operation(summary = "Simulate a workload against the Task Queue",
          tags = {"tasks"},
          responses = {
                  @ApiResponse(content = @Content(schema = @Schema(implementation = int.class))),
                  @ApiResponse(responseCode = "405", description = "Invalid input")
          })
  public int simulateWorkload(
//          @Parameter(description = "The groupId for this simulation workload", required = true)  @PathParam("groupId") String groupId,
          @Parameter(description = "Total number of tasks to submit", required = true)  @PathParam("numberOfTasks") int numberOfTasks,
          @Parameter(description = "A rough duration for each tasks interval", required = true)  @PathParam("durationSeconds") int durationSeconds,
          @Parameter(description = "Number of workers to parallelize the simulation through", required = true)  @PathParam("numberOfWorkers") int numberOfWorkers
  ) {

    return new Simulator(kwqInstance.getKwq(), kwqInstance.getTaskStatus()).simulate("GroupId- " + new Date().toString(), numberOfTasks, durationSeconds, numberOfWorkers);
  }
}