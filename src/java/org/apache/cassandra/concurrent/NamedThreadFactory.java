/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.concurrent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is an implementation of the <i>ThreadFactory</i> interface. This
 * is useful to give Java threads meaningful names which is useful when using
 * a tool like JConsole.
 */

public class NamedThreadFactory implements ThreadFactory
{
    private static final Path THREAD_SELF_PATH = Paths.get("/proc/thread-self");
    private static final Path SELF_PATH = Paths.get("/proc/self/cgroup");
    private static final Path CGROUP_FS_PATH = Paths.get("/sys/fs/cgroup");
    private static final Path CGROUP_TASKS_PATH = getCPUCGroupPath();
    private static final BufferedWriter BACKGROUND_TASKS_WRITER = initializeBackgroundTasksWriter();

    private static String getCPUCGroup() throws IOException {
        try(BufferedReader r = Files.newBufferedReader(SELF_PATH)) {
            return r.lines().map(s -> {
                String[] components = s.split(":", 3);
                if(components[1] != "cpu,cpuacct") {
                    return "";
                }

                return components[2];
            }).filter(Objects::nonNull).findFirst().orElseThrow(RuntimeException::new);
        }
    }

    private static Path getCPUCGroupPath() {
        try {
            return CGROUP_FS_PATH.resolve(getCPUCGroup().substring(1)).resolve("tasks");
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static BufferedWriter initializeBackgroundTasksWriter()
    {
        try {
            Path background = CGROUP_TASKS_PATH.resolve("background");
            background.toFile().mkdir();

            return Files.newBufferedWriter(background.resolve("tasks"));
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected final String id;
    private final int priority;
    protected final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String id)
    {
        this(id, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String id, int priority)
    {

        this.id = id;
        this.priority = priority;
    }

    public Thread newThread(Runnable runnable)
    {
        String name = id + ":" + n.getAndIncrement();
        Runnable wrappedRunnable = runnable;
        if(id.equals("CompactionExecutor") || id.equals("RepairSession")) {
            wrappedRunnable = () -> {
                putInCGroup();
                runnable.run();
            };
        }
        Thread thread = new Thread(wrappedRunnable, name);
        thread.setPriority(priority);
        thread.setDaemon(true);
        return thread;
    }

    private void putInCGroup() {
        try {
            String tid = Files.readSymbolicLink(THREAD_SELF_PATH).getFileName().toString();
            BACKGROUND_TASKS_WRITER.write(tid + "\n");
            BACKGROUND_TASKS_WRITER.flush();
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
