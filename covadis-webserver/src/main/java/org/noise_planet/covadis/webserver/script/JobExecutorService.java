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

import java.util.Map;
import java.util.concurrent.*;

/**
 * Manage pool of Job Threads.
 */
public class JobExecutorService {
    private final Map<Integer, Job<?>> jobs = new ConcurrentHashMap<>();
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;

    public JobExecutorService(int corePoolSize, int maximumPoolSize, long keepAliveTime, @NotNull TimeUnit unit) {
        this.executorService = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                new SynchronousQueue<>(), Executors.defaultThreadFactory());
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    public <T> Future<T> submitJob(Job<T> job) {
        if (jobs.containsKey(job.getId())) {
            throw new IllegalArgumentException(String.format("Job with ID %d already exists.", job.getId()));
        }
        jobs.put(job.getId(), job);
        Future<T> futureTask = executorService.submit(job);
        job.setFuture(futureTask);
        return futureTask;
    }

    public void cancelJob(int jobId, int abortDelay) {
        Job<?> job = jobs.get(jobId);
        if (job != null) {
            job.cancel();
            // After a specified delay, abort the process if it can't handle the progress monitor cancel
            scheduledExecutorService.schedule(() -> {
                if (job.isRunning() && job.getFuture() != null) {
                    job.getFuture().cancel(true);
                }
            }, abortDelay, TimeUnit.SECONDS);
        }
        jobs.remove(jobId);
    }

    public void shutdown() {
        executorService.shutdown();
        scheduledExecutorService.shutdown();
    }

}