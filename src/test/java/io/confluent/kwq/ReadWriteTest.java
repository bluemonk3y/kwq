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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class ReadWriteTest {


  private IntegrationTestHarness testHarness;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
  }

  @After
  public void after() {

    testHarness.stop();

  }

  @Test
  public void testMsgInAndOut() throws Exception {

    testHarness.createTopic("TEST", 1, 1);


    testHarness.produceData("TEST", TaskDataProvider.data, new TaskSerDes(), System.currentTimeMillis());

    Map<String, Task> results = testHarness.consumeData("TEST", 1, new StringDeserializer(), new TaskSerDes(), 1000);

    for (Map.Entry<String, Task> stringGenericRowEntry : results.entrySet()) {
      System.out.println(stringGenericRowEntry.getValue());
    }
    Assert.assertEquals(TaskDataProvider.data.size(), results.size());
  }
}
