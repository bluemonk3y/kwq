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

    TaskDataProvider taskDataProvider = new TaskDataProvider();


    testHarness.produceData("TEST", taskDataProvider.data, new TaskSerDes(), System.currentTimeMillis());

    Map<String, Task> results = testHarness.consumeData("TEST", 1, new StringDeserializer(), new TaskSerDes(), 1000);

    for (Map.Entry<String, Task> stringGenericRowEntry : results.entrySet()) {
      System.out.println(stringGenericRowEntry.getValue());
    }
    Assert.assertEquals(taskDataProvider.data.size(), results.size());
  }
}
