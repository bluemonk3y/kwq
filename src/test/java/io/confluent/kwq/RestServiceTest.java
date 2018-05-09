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

public class RestServiceTest {


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
  public void testEndpointConnectsToKafka() throws Exception {

    System.setProperty("bootstrap.servers", testHarness.embeddedKafkaCluster.bootstrapServers());
    RestServerMain.main(null);

    Thread.sleep(10 * 1000);

    KwqInstance instance = KwqInstance.getInstance(null);
    Assert.assertNotNull("Should have created KSWQ instance", instance);


  }
}
