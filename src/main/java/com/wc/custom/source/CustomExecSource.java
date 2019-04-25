/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wc.custom.source;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 主要修改代码：
 * 1.在每一行日志的前部加上机器名称 和 服务名称
 * 2.机器名称和服务名称从配置文件中读取，假如没有指定，就使用默认
 */
public class CustomExecSource extends AbstractSource implements EventDrivenSource,
Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(CustomExecSource.class);

  private String shell;
  private String command;
  private SourceCounter sourceCounter;
  private ExecutorService executor;
  private Future<?> runnerFuture;
  private long restartThrottle;
  private boolean restart;
  private boolean logStderr;
  private Integer bufferCount;
  private long batchTimeout;
  private ExecRunnable runner;
  private Charset charset;
  //机器名称  服务名称
  private String hostName;
  private String serviceName;

  @Override
  public void start() {
    logger.info("Exec source starting with command:{}", command);

    executor = Executors.newSingleThreadExecutor();

    runner = new ExecRunnable(shell, command, getChannelProcessor(), sourceCounter,
        restart, restartThrottle, logStderr, bufferCount, batchTimeout, charset,hostName,serviceName);

    // FIXME: Use a callback-like executor / future to signal us upon failure.
    runnerFuture = executor.submit(runner);

    /*
     * NB: This comes at the end rather than the beginning of the method because
     * it sets our state to running. We want to make sure the executor is alive
     * and well first.
     */
    sourceCounter.start();
    super.start();

    logger.debug("Exec source started");
  }

  @Override
  public void stop() {
    logger.info("Stopping exec source with command:{}", command);
    if(runner != null) {
      runner.setRestart(false);
      runner.kill();
    }

    if (runnerFuture != null) {
      logger.debug("Stopping exec runner");
      runnerFuture.cancel(true);
      logger.debug("Exec runner stopped");
    }
    executor.shutdown();

    while (!executor.isTerminated()) {
      logger.debug("Waiting for exec executor service to stop");
      try {
        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while waiting for exec executor service "
            + "to stop. Just exiting.");
        Thread.currentThread().interrupt();
      }
    }

    sourceCounter.stop();
    super.stop();

    logger.debug("Exec source with command:{} stopped. Metrics:{}", command,
        sourceCounter);
  }

  @Override
  public void configure(Context context) {
    command = context.getString("command");

    Preconditions.checkState(command != null,
        "The parameter command must be specified");

    restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
        ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

    restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,
        ExecSourceConfigurationConstants.DEFAULT_RESTART);

    logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR,
        ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);

    bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
        ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

    batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
        ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

    charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET,
        ExecSourceConfigurationConstants.DEFAULT_CHARSET));

    shell = context.getString(ExecSourceConfigurationConstants.CONFIG_SHELL, null);

    // 在此方法中，通过context.getString("property", defaultValue)
    // 读取参数配置  ,没有指定就默认配置
    hostName= context.getString("hostName","xxx");
    serviceName=context.getString("serviceName", "xxx");

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  private static class ExecRunnable implements Runnable {

    public ExecRunnable(String shell, String command, ChannelProcessor channelProcessor,
        SourceCounter sourceCounter, boolean restart, long restartThrottle,
        boolean logStderr, int bufferCount, long batchTimeout, Charset charset,
                        String hostName,String serviceName) {
      this.command = command;
      this.channelProcessor = channelProcessor;
      this.sourceCounter = sourceCounter;
      this.restartThrottle = restartThrottle;
      this.bufferCount = bufferCount;
      this.batchTimeout = batchTimeout;
      this.restart = restart;
      this.logStderr = logStderr;
      this.charset = charset;
      this.shell = shell;
      this.hostName=hostName;
      this.serviceName=serviceName;
    }

    private final String shell;
    private final String command;
    private final ChannelProcessor channelProcessor;
    private final SourceCounter sourceCounter;
    private volatile boolean restart;
    private final long restartThrottle;
    private final int bufferCount;
    private long batchTimeout;
    private final boolean logStderr;
    private final Charset charset;
    private Process process = null;
    private SystemClock systemClock = new SystemClock();
    private Long lastPushToChannel = systemClock.currentTimeMillis();
    ScheduledExecutorService timedFlushService;
    ScheduledFuture<?> future;
    //机器名称  服务名称
    private String hostName;
    private String serviceName;
    //临时行
    private String tmpLine;


    @Override
    public void run() {
      do {
        String exitCode = "unknown";
        BufferedReader reader = null;
        String line = null;
        final List<Event> eventList = new ArrayList<Event>();

        timedFlushService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(
                "timedFlushExecService" +
                Thread.currentThread().getId() + "-%d").build());
        try {
          if(shell != null) {
            String[] commandArgs = formulateShellCommand(shell, command);
            process = Runtime.getRuntime().exec(commandArgs);
          }  else {
            String[] commandArgs = command.split("\\s+");
            process = new ProcessBuilder(commandArgs).start();
          }
          reader = new BufferedReader(
              new InputStreamReader(process.getInputStream(), charset));

          // StderrLogger dies as soon as the input stream is invalid
          StderrReader stderrReader = new StderrReader(new BufferedReader(
              new InputStreamReader(process.getErrorStream(), charset)), logStderr);
          stderrReader.setName("StderrReader-[" + command + "]");
          stderrReader.setDaemon(true);
          stderrReader.start();

          future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
              @Override
              public void run() {
                try {
                  synchronized (eventList) {
                    if(!eventList.isEmpty() && timeout()) {
                      flushEventBatch(eventList);
                    }
                  }
                } catch (Exception e) {
                  logger.error("Exception occured when processing event batch", e);
                  if(e instanceof InterruptedException) {
                      Thread.currentThread().interrupt();
                  }
                }
              }
          },
          batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);

          while ((line = reader.readLine()) != null) {
            synchronized (eventList) {
              sourceCounter.incrementEventReceivedCount();
              //判断line是否含有日志级别的标志符，假如含有，说明是第一行，则添加机器名称和服务名称
              if(line.contains("INFO")==true || line.contains("WARN")==true || line.contains("ERROR")==true || line.contains("DEBUG")==true){
                tmpLine=hostName+" "+serviceName+" "+line;
              }else if(line == null || "".trim().equals(line)) { //判断是否为空白行，则结束本次循环，继续下一次循环
                continue;
              }else{ //否则为原来输出行
                tmpLine=line;
              }
              //将一行写入eventList
              eventList.add(EventBuilder.withBody(tmpLine.getBytes(charset)));
              if(eventList.size() >= bufferCount || timeout()) {
                flushEventBatch(eventList);
              }
            }
          }

          synchronized (eventList) {
              if(!eventList.isEmpty()) {
                flushEventBatch(eventList);
              }
          }
        } catch (Exception e) {
          logger.error("Failed while running command: " + command, e);
          if(e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException ex) {
              logger.error("Failed to close reader for exec source", ex);
            }
          }
          exitCode = String.valueOf(kill());
        }
        if(restart) {
          logger.info("Restarting in {}ms, exit code {}", restartThrottle,
              exitCode);
          try {
            Thread.sleep(restartThrottle);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        } else {
          logger.info("Command [" + command + "] exited with " + exitCode);
        }
      } while(restart);
    }

    private void flushEventBatch(List<Event> eventList){
      channelProcessor.processEventBatch(eventList);
      sourceCounter.addToEventAcceptedCount(eventList.size());
      eventList.clear();
      lastPushToChannel = systemClock.currentTimeMillis();
    }

    private boolean timeout(){
      return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
    }

    private static String[] formulateShellCommand(String shell, String command) {
      String[] shellArgs = shell.split("\\s+");
      String[] result = new String[shellArgs.length + 1];
      System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
      result[shellArgs.length] = command;
      return result;
    }

    public int kill() {
      if(process != null) {
        synchronized (process) {
          process.destroy();

          try {
            int exitValue = process.waitFor();

            // Stop the Thread that flushes periodically
            if (future != null) {
                future.cancel(true);
            }

            if (timedFlushService != null) {
              timedFlushService.shutdown();
              while (!timedFlushService.isTerminated()) {
                try {
                  timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  logger.debug("Interrupted while waiting for exec executor service "
                    + "to stop. Just exiting.");
                  Thread.currentThread().interrupt();
                }
              }
            }
            return exitValue;
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
        return Integer.MIN_VALUE;
      }
      return Integer.MIN_VALUE / 2;
    }
    public void setRestart(boolean restart) {
      this.restart = restart;
    }
  }
  private static class StderrReader extends Thread {
    private BufferedReader input;
    private boolean logStderr;

    protected StderrReader(BufferedReader input, boolean logStderr) {
      this.input = input;
      this.logStderr = logStderr;
    }

    @Override
    public void run() {
      try {
        int i = 0;
        String line = null;
        while((line = input.readLine()) != null) {
          if(logStderr) {
            // There is no need to read 'line' with a charset
            // as we do not to propagate it.
            // It is in UTF-16 and would be printed in UTF-8 format.
            logger.info("StderrLogger[{}] = '{}'", ++i, line);
          }
        }
      } catch (IOException e) {
        logger.info("StderrLogger exiting", e);
      } finally {
        try {
          if(input != null) {
            input.close();
          }
        } catch (IOException ex) {
          logger.error("Failed to close stderr reader for exec source", ex);
        }
      }
    }
  }
}
