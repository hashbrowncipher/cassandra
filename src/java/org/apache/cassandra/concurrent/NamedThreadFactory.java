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
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;

/**
 * This class is an implementation of the <i>ThreadFactory</i> interface. This
 * is useful to give Java threads meaningful names which is useful when using
 * a tool like JConsole.
 */

public class NamedThreadFactory implements ThreadFactory
{
    private static final Path THREAD_SELF_PATH = Paths.get("/proc/thread-self");
    private static final Path SELF_PATH = Paths.get("/proc/self/cgroup");
    private static final Path CGROUP_FS_PATH = Paths.get("/sys/fs/cgroup/cpu,cpuacct");
    private static final Path CGROUP_TASKS_PATH = getCPUCGroupPath();
    private static final BufferedWriter BACKGROUND_TASKS_WRITER = initializeBackgroundTasksWriter();

    private static volatile String globalPrefix;
    public static void setGlobalPrefix(String prefix) { globalPrefix = prefix; }

    public final String id;


    private static String getCPUCGroup() throws IOException {
        try(BufferedReader r = Files.newBufferedReader(SELF_PATH, StandardCharsets.UTF_8)) {
            while(true) {
                String line = r.readLine();
                if(line == null) {
                    throw new RuntimeException();
                }

                String[] components = line.split(":", 3);
                System.err.println(components[1]);
                if(components[1].equals("cpu,cpuacct")) {
                    return components[2];
                }
            }
        }
    }

    private static Path getCPUCGroupPath() {
        try {
            return CGROUP_FS_PATH.resolve(getCPUCGroup().substring(1));
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static BufferedWriter initializeBackgroundTasksWriter()
    {
        try {
            Path background = CGROUP_TASKS_PATH.resolve("background");

            return new BufferedWriter(new FileWriter(background.resolve("tasks").toFile()));
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final int priority;
    private final ClassLoader contextClassLoader;
    private final ThreadGroup threadGroup;
    protected final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String id)
    {
        this(id, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String id, int priority)
    {
        this(id, priority, null, null);
    }

    public NamedThreadFactory(String id, int priority, ClassLoader contextClassLoader, ThreadGroup threadGroup)
    {
        this.id = id;
        this.priority = priority;
        this.contextClassLoader = contextClassLoader;
        this.threadGroup = threadGroup;
    }

    public Thread newThread(Runnable runnable)
    {
        String name = id + ':' + n.getAndIncrement();
        String prefix = globalPrefix;

        Thread thread;
        if(id.equals("CompactionExecutor") || id.equals("ValidationExecutor") || id.equals("StreamReceiveTask") || id.startsWith("Repair#")) {
            thread = new Thread(cgroupRunnable(runnable), name);
        } else {
            thread = new FastThreadLocalThread(threadGroup, threadLocalDeallocator(runnable), prefix != null ? prefix + name : name);
        }

        thread.setPriority(priority);
        thread.setDaemon(true);
        if (contextClassLoader != null)
            thread.setContextClassLoader(contextClassLoader);
        return thread;
    }

    /**
     * Ensures that {@link FastThreadLocal#remove() FastThreadLocal.remove()} is called when the {@link Runnable#run()}
     * method of the given {@link Runnable} instance completes to ensure cleanup of {@link FastThreadLocal} instances.
     * This is especially important for direct byte buffers allocated locally for a thread.
     */
    public static Runnable threadLocalDeallocator(Runnable r)
    {
        return () -> {
            try {
                r.run();
            } finally {
                FastThreadLocal.removeAll();
            }
        };
    }

    private static Runnable cgroupRunnable(Runnable r) {
        return () -> {
                try {
                    String tid = Files.readSymbolicLink(THREAD_SELF_PATH).getFileName().toString();
                    BACKGROUND_TASKS_WRITER.write(tid + "\n");
                    BACKGROUND_TASKS_WRITER.flush();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

            r.run();
        };
    }
}
