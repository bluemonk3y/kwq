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

import io.confluent.kwq.utils.IntegrationTestHarness;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class SimpleKwqTaskPriorityTest {


  private IntegrationTestHarness testHarness;
  private SimpleKwq kwq;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();

    kwq = new SimpleKwq(5, "KWQ", testHarness.embeddedKafkaCluster.bootstrapServers(), 1, (short)1);

    kwq.start();
  }

  @After
  public void after() {
    testHarness.stop();
  }


  @Test
  public void serviceHighestPriorityTask() throws Exception {


    Map<String, Task> testData = TaskDataProvider.data;

    for (Map.Entry<String, Task> stringTaskEntry : testData.entrySet()) {
      testHarness.produceData("KWQ-" + stringTaskEntry.getKey(), Collections.singletonMap(stringTaskEntry.getKey(), stringTaskEntry.getValue()), new TaskSerDes(), 1L);
    }

    Task first = kwq.consume();
    Assert.assertEquals(testData.get("5"), first);
    System.out.println(first);
  }

  @Test
  public void serviceSingleTask() throws Exception {

    Task firstItem = TaskDataProvider.data.values().iterator().next();

    testHarness.produceData("KWQ-5", Collections.singletonMap(firstItem.getId(), firstItem), new TaskSerDes(), 1L);

    Task consume = kwq.consume();
    Assert.assertEquals(firstItem, consume);
    System.out.println(consume);
  }


// TODO: TEST FAILS consistently - explore why 10 consumers makes it break... parked for now
//  @Test
//  public void shouldWorkWith_TEN_ButDoesnt() throws Exception {
//
//    // When setting P to 10 - the consumers fail to retrieve results
//    SimpleKwq kwq = new SimpleKwq(10, "KWQ", testHarness.embeddedKafkaCluster.bootstrapServers(), 1, (short)1);
//
//    kwq.start();
//
//    Task firstItem = TaskDataProvider.data.values().iterator().next();
//
//    testHarness.produceData("KWQ-9", Collections.singletonMap(firstItem.getId(), firstItem), new TaskSerDes(), 1l );
//
//    Task consume = kwq.consume();
//    Assert.assertEquals(firstItem, consume);
//    System.out.println(consume);
//  }
}
