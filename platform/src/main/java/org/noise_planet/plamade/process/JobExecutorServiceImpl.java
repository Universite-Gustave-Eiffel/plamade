package org.noise_planet.plamade.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class JobExecutorServiceImpl extends ThreadPoolExecutor implements JobExecutorService {
    ArrayList<NoiseModellingInstance> instances = new ArrayList<>();

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public List<NoiseModellingInstance> getNoiseModellingInstance() {
        return Collections.unmodifiableList(new ArrayList<>(instances));
    }

    @Override
    public void execute(Runnable command) {
        // add runnable in list
        if(command instanceof NoiseModellingInstance) {
            instances.add((NoiseModellingInstance) command);
        }
        super.execute(command);
    }
}
