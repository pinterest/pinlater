/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.pinlater.commons.util;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.ByteBuffer;
import java.util.Formatter;

public class BytesUtil {

  /**
   * Converts a hex string to bytes.
   *
   * @param s a hex string
   * @return byte array
   */
  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  /**
   * Converts a byte array to hex string.
   *
   * @param bytes a byte array.
   * @param pos   the start position
   * @param len   number of bytes to be converted
   * @return hex string.
   */
  public static String byteArrayToHexString(byte[] bytes, int pos, int len) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    Formatter formatter = new Formatter(sb);
    for (int i = pos; i < pos + len; ++i) {
      formatter.format("%02x", bytes[i]);
    }
    return sb.toString();
  }

  /**
   * Reads the remaining bytes in a ByteBuffer into a byte[].
   *
   * @param byteBuffer byte buffer to read from.
   * @return byte[] containing the bytes read.
   */
  public static byte[] readBytesFromByteBuffer(ByteBuffer byteBuffer) {
    byte[] buffer = new byte[byteBuffer.remaining()];
    byteBuffer.get(buffer);
    return buffer;
  }

  /**
   * Reads the remaining bytes in a ByteBuffer into a byte[] without consuming.
   *
   * @param byteBuffer byte buffer to read from.
   * @return byte[] containing the bytes read.
   */
  public static byte[] readBytesFromByteBufferWithoutConsume(ByteBuffer byteBuffer) {
    byte[] buffer = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(buffer);
    return buffer;
  }

  public static ByteBuffer stringToByteBuffer(String str) {
    return ByteBuffer.wrap(str.getBytes(Charsets.UTF_8));
  }

  public static String stringFromByteBuffer(ByteBuffer byteBuf) {
    return new String(readBytesFromByteBuffer(byteBuf), Charsets.UTF_8);
  }

  public static ByteBuffer longToByteBuffer(long num) {
    return ByteBuffer.allocate(Long.SIZE / 8).putLong(0, num);
  }

  public static long longFromByteBuffer(ByteBuffer byteBuffer) {
    return ByteBuffer.wrap(readBytesFromByteBuffer(byteBuffer)).getLong();
  }

  public static ByteBuffer intToByteBuffer(int num) {
    return ByteBuffer.allocate(Integer.SIZE / 8).putInt(0, num);
  }

  public static int intFromByteBuffer(ByteBuffer byteBuffer) {
    return ByteBuffer.wrap(readBytesFromByteBuffer(byteBuffer)).getInt();
  }

  public static ByteBuffer doubleToByteBuffer(double val) {
    return ByteBuffer.allocate(Double.SIZE / 8).putDouble(val);
  }

  public static double doubleFromByteBuffer(ByteBuffer byteBuffer) {
    return ByteBuffer.wrap(readBytesFromByteBuffer(byteBuffer)).getDouble();
  }

  public static ByteBuffer byteToByteBuffer(byte num) {
    return ByteBuffer.allocate(1).put(num);
  }

  public static byte byteFromByteBuffer(ByteBuffer byteBuffer) {
    return ByteBuffer.wrap(readBytesFromByteBuffer(byteBuffer)).get();
  }

  public static byte[] toBytes(long num) {
    return readBytesFromByteBuffer(longToByteBuffer(num));
  }

  public static byte[] toBytes(String str) {
    return readBytesFromByteBuffer(stringToByteBuffer(str));
  }

  /**
   * Converts UNREAD BYTES to byte array from ChannelBuffer
   * NOTE: this will consume all the readable bytes from channel buffer
   */
  public static byte[] toBytesWithoutConsume(ChannelBuffer input) {
    // save old reader's index & reset it
    int oldIndex = input.readerIndex();
    input.resetReaderIndex();
    // read bytes out
    byte[] output = new byte[input.readableBytes()];
    input.readBytes(output);
    // set reader's index back to
    input.readerIndex(oldIndex);
    return output;
  }
}
