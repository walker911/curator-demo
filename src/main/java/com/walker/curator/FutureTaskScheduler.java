package com.walker.curator;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *
 * </p>
 *
 * @author mu qin
 * @date 2020/1/15
 */
public class FutureTaskScheduler extends Thread {

    private ConcurrentLinkedQueue<ExecuteTask> executeTaskQueue = new ConcurrentLinkedQueue<>();
    private long sleepTime = 200;
    private ExecutorService pool = Executors.newFixedThreadPool(10);
    private static FutureTaskScheduler inst = new FutureTaskScheduler();

    private FutureTaskScheduler() {
        this.start();
    }

    public static void add(ExecuteTask task) {
        inst.executeTaskQueue.add(task);
    }

    @Override
    public void run() {
        while (true) {
            handleTask();
            threadSleep(sleepTime);
        }
    }

    private void threadSleep(long time) {
        try {
            sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 处理任务队列，检查其中是否有任务
     */
    private void handleTask() {
        try {
            ExecuteTask executeTask;
            while (executeTaskQueue.peek() != null) {
                executeTask = executeTaskQueue.poll();
                handleTask(executeTask);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleTask(ExecuteTask executeTask) {
        pool.execute(new ExecuteRunnable(executeTask));
    }

    static class ExecuteRunnable implements Runnable {

        ExecuteTask executeTask;

        public ExecuteRunnable(ExecuteTask executeTask) {
            this.executeTask = executeTask;
        }

        @Override
        public void run() {
            executeTask.execute();
        }
    }
}
