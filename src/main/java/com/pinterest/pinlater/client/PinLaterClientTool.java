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
package com.pinterest.pinlater.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client tool to talk to PinLater. This can be used for correctness as well as
 * load/performance testing.
 */
public class PinLaterClientTool {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterClientTool.class);

  private static Options getOptions() {
    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt("host").isRequired().hasArg()
        .withDescription("PinLater server hostname").create());
    options.addOption(OptionBuilder.withLongOpt("port").isRequired().hasArg()
        .withType(Number.class).withDescription("PinLater server port").create());

    options.addOption(OptionBuilder.withLongOpt("mode").isRequired().hasArg()
        .withDescription("Mode: create, enqueue, dequeue, check_dequeue, lookup, "
            + "get_job_count, get_queue_names").create());
    options.addOption(OptionBuilder.withLongOpt("queue").hasArg()
        .withDescription("Queue name").create());
    options.addOption(OptionBuilder.withLongOpt("concurrency").hasArg()
        .withDescription("Query issue concurrency").create());
    options.addOption(OptionBuilder.withLongOpt("batch_size").hasArg()
        .withDescription("Query batch size").create());
    options.addOption(OptionBuilder.withLongOpt("num_queries").hasArg()
        .withDescription("How many enqueue/dequeue queries to issue, -1 for unlimited").create());
    options.addOption(OptionBuilder.withLongOpt("dequeue_success_percent").hasArg()
        .withDescription("Dequeue success percent to use").create());
    options.addOption(OptionBuilder.withLongOpt("job_descriptor").hasArg()
        .withDescription("Job descriptor to lookup").create());
    options.addOption(OptionBuilder.withLongOpt("priority").hasArg()
        .withDescription("Job priority for enqueue").create());
    options.addOption(OptionBuilder.withLongOpt("job_state").hasArg()
        .withDescription("Job state to search for").create());
    options.addOption(OptionBuilder.withLongOpt("count_future_jobs").hasArg()
        .withDescription("Count future jobs instead of current jobs").create());

    return options;
  }

  public static void main(String[] args) {
    try {
      CommandLineParser parser = new PosixParser();
      CommandLine cmdLine = parser.parse(getOptions(), args);
      new PinLaterQueryIssuer(cmdLine).run();
    } catch (Exception e) {
      LOG.error("Exception in client tool", e);
      System.exit(1);
    }
  }
}
