package com.pinterest.pinlater.backends.mysql;

import org.junit.Assert;
import org.junit.Test;

public class MySQLBackendUtilsTest {

  @Test
  public void testconstructDBName() {
    validateQueueName("test");
    validateQueueName("test_queue");
    validateQueueName("_test_");
  }

  private void validateQueueName(String queueName) {
    String shardName = MySQLBackendUtils.constructShardName(1, 0);
    String dbName = MySQLBackendUtils.constructDBName(queueName, shardName);
    // Database 0's name should be backward compatible.
    Assert.assertEquals(MySQLBackendUtils.constructDBName(queueName, "1"), dbName);
    Assert.assertEquals(queueName, MySQLBackendUtils.queueNameFromDBName(dbName));
    Assert.assertEquals(shardName, MySQLBackendUtils.shardNameFromDBName(dbName));

    shardName = MySQLBackendUtils.constructShardName(1, 1);
    dbName = MySQLBackendUtils.constructDBName(queueName, shardName);
    Assert.assertEquals(queueName, MySQLBackendUtils.queueNameFromDBName(dbName));
    Assert.assertEquals(shardName, MySQLBackendUtils.shardNameFromDBName(dbName));
  }
}
