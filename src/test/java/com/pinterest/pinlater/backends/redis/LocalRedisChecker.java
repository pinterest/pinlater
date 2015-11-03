package com.pinterest.pinlater.backends.redis;

import com.google.common.collect.Maps;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

/**
 * Class to check if redis is running on localhost.
 */
public class LocalRedisChecker {

  private static Map<Integer, Boolean> sIsRunning = Maps.newConcurrentMap();

  public static boolean isRunning(int port) {
    if (sIsRunning.containsKey(port)) {
      return sIsRunning.get(port);
    }
    synchronized (sIsRunning) {
      Jedis conn = null;
      try {
        conn = new Jedis("localhost", port);
        conn.ping();
        sIsRunning.put(port, true);
      } catch (JedisConnectionException e) {
        sIsRunning.put(port, false);
      } finally {
        conn.disconnect();
      }
      return sIsRunning.get(port);
    }
  }
}
