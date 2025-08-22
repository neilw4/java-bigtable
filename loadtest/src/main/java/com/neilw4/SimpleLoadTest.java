/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.neilw4;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SimpleLoadTest {

  private static final String PROJECT_ID = "google.com:cloud-bigtable-dev";
  private static final String INSTANCE_ID = "neilwells-test";
  private static final String TABLE_NAME = "neilwells-test-table";
  private static final TableId TABLE_ID = TableId.of(TABLE_NAME);

  private static final int NUM_THREADS = 50;
  private static final int RUN_DURATION_SECONDS = 15;
  private static final int WARMUM_TIME_S = 3;
  private static final int SECONDS_BETWEEN_TESTS = 5;
  private static final int[] QPS_TARGETS = { 50, 100, 200, 500, 1_000, 2_500,
                                            5_000, 10_000, 25_000, 50_000};

  private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS: ");

  public static void log(String l) {
    System.err.println(TIME_FORMATTER.format(LocalDateTime.now()) + l);
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .build();
    BigtableTableAdminClient adminClient = BigtableTableAdminClient.create(adminSettings);
    if (adminClient.exists(TABLE_NAME)) {
      log("tearing down table");
      adminClient.deleteTable(TABLE_NAME);
    }
    log("Creating table");
    CreateTableRequest createTableRequest =
        CreateTableRequest.of(TABLE_NAME).addFamily("col1");
    adminClient.createTable(createTableRequest);
    adminClient.close();
  
    log("connecting");

    BigtableDataSettings settings =
        BigtableDataSettings.newBuilder().setProjectId(PROJECT_ID).setInstanceId(INSTANCE_ID).build();

    BigtableDataClient dataClient = BigtableDataClient.create(settings);

    log("populating");
    RowMutation rowMutation =
        RowMutation.create(TABLE_ID, "myrow")
            .setCell("col1", "colq1", "val");
    dataClient.mutateRow(rowMutation);

    System.out.println("target,throughput,mean,p50,p90,p95,p99");
    for (int targetQps : QPS_TARGETS) {
      runTest(dataClient, targetQps);
      TimeUnit.SECONDS.sleep(SECONDS_BETWEEN_TESTS);
    }
    dataClient.close();
    log("goodbye");
  }


  private static void runTest(BigtableDataClient client, int targetQps)
      throws InterruptedException {
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_THREADS);
    final List<List<Double>> allLatencies = new ArrayList<>();
    
    final AtomicBoolean isRunning = new AtomicBoolean(true);
    int qpsPerThread = targetQps / NUM_THREADS;

    log("starting run with QPS " + targetQps);
    for (int i = 0; i < NUM_THREADS; i++) {
      List<Double> latencies = new ArrayList<>(targetQps * (RUN_DURATION_SECONDS + 5));
      allLatencies.add(latencies);
      executor.submit(
          new LoadWorker(client, qpsPerThread, isRunning, latencies));
    }

    TimeUnit.SECONDS.sleep(WARMUM_TIME_S + RUN_DURATION_SECONDS);
    isRunning.set(false);
    log("Finished run");
    executor.shutdown();
    log("Shut down run");

    log("combining latencies");
    double[] latencies = allLatencies.stream().flatMap(List::stream).mapToDouble(Double::doubleValue).sorted().toArray();
    log("Calculating stats");
    double throughput = latencies.length / RUN_DURATION_SECONDS;
    if (latencies.length > 0) {
      double meanLatency = (double) Arrays.stream(latencies).average().getAsDouble();
      double p50 = latencies[(int) (latencies.length * 0.50)];
      double p90 = latencies[(int) (latencies.length * 0.90)];
      double p95 = latencies[(int) (latencies.length * 0.95)];
      double p99 = latencies[(int) (latencies.length * 0.99)];

      System.out.println(targetQps+","+throughput+","+meanLatency+","+p50+","+p90+","+p95+","+p99);
    } else {
      log("no data for run with QPS" + targetQps);
    }
  }


  static class LoadWorker implements Runnable {
    private final BigtableDataClient dataClient;
    private final long targetQpsPerThread;
    private final AtomicBoolean isRunning;
    private final List<Double> latencies;

    LoadWorker(
        BigtableDataClient dataClient,
        long targetQpsPerThread,
        AtomicBoolean isRunning,
        List<Double> latencies) {
      this.dataClient = dataClient;
      this.targetQpsPerThread = targetQpsPerThread;
      this.isRunning = isRunning;
      this.latencies = latencies;
    }

    static class RowCallback implements Runnable {

      private final long startNs;
      private final List<Double> latencies;
      private final AtomicBoolean isRunning;

      RowCallback(long startNs, List<Double> latencies, AtomicBoolean isRunning) {
        this.startNs = startNs;
        this.latencies = latencies;
        this.isRunning = isRunning;
      }

      @Override
      public void run() {
        if (isRunning.get()) {
          long latencyNs = System.nanoTime() - startNs;
          double latencyMs = latencyNs / 1000.0 / 1000.0;
          latencies.add(latencyMs);
        }
      }
    }

    @Override
    public void run() {
      ThreadPoolExecutor callbackExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
      final long periodNs = (1000 * 1000  * 1000) / targetQpsPerThread;
      boolean warming = true;
      final long warmupEndMs = System.currentTimeMillis() + WARMUM_TIME_S * 1000;

      while (isRunning.get()) {
        try {
          long startNs = System.nanoTime();
          ApiFuture<Row> rowFuture = dataClient.readRowAsync(TABLE_ID, "myrow");

          if (warming) {
            warming = System.currentTimeMillis() <= warmupEndMs;
          }
          if (!warming) {
            rowFuture.addListener(new RowCallback(startNs, latencies, isRunning), callbackExecutor);
          }

          long waitPeriodNs = startNs + periodNs - System.nanoTime();
          long waitPeriodMs = waitPeriodNs / 1000 / 1000;
          // Sleep to maintain the target QPS
          if (waitPeriodMs > 0) {
            Thread.sleep(waitPeriodMs);
          } else {
            // Give other threads time to do things.
            Thread.yield();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          log("An error occurred in a worker thread: " + e.getMessage());
        }
      }
      // We'd like to shut this down, but we get problems because there's a tail of long-running requests
     // that try to execute the callback on the executor for a long time after shutting it down.
      // try {
      //   Thread.sleep(SECONDS_BETWEEN_TESTS);
      // } catch (InterruptedException e) {
      //   Thread.currentThread().interrupt();
      // }
      // callbackExecutor.shutdown();
    }
  }
}
