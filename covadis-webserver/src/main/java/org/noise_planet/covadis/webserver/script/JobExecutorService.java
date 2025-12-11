/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.covadis.webserver.script;

import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Manage pool of Job Threads.
 */
public class JobExecutorService extends ThreadPoolExecutor {
    private final Map<Integer, Future<?>> jobs = new ConcurrentHashMap<>();

    public JobExecutorService(int corePoolSize, int maximumPoolSize, long keepAliveTime, @NotNull TimeUnit unit, @NotNull BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @NotNull
    @Override
    public <T> Future<T> submit(@NotNull Callable<T> task) {
        Future<T> future = super.submit(task);
        if(task instanceof Job) {
            Job<T> job = (Job<T>) task;
            job.setFuture(future);
            jobs.put(job.getId(), (Job<Object>) job);
        }
        return future;
    }

}