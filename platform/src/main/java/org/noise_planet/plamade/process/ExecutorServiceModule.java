/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.plamade.process;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Generate an instance of ThreadPoolExecutor.
 *
 * @author Nicolas Fortin, Université Gustave Eiffel
 */
public class ExecutorServiceModule extends AbstractModule implements Provider<ThreadPoolExecutor> {

    public static final int CORE_POOL_SIZE = 4;
    public static final int MAXIMUM_POOL_SIZE = 32;
    public static final int KEEP_ALIVE_TIME = 30;
    BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(MAXIMUM_POOL_SIZE);

    @Override
    protected void configure() {
        bind(ThreadPoolExecutor.class).toProvider(this).in(Scopes.SINGLETON);
    }

    @Override
    public ThreadPoolExecutor get() {
        return new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS, workQueue);
    }
}
