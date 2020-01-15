package com.walker.curator.lock;

import com.walker.curator.FutureTaskScheduler;
import com.walker.curator.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.Test;

/**
 * <p>
 * 测试分布式锁
 * </p>
 *
 * @author mu qin
 * @date 2020/1/15
 */
public class ZkLockTester {

    int count = 0;

    @Test
    public void testLock() {
        for (int i = 0; i < 10; i++) {
            FutureTaskScheduler.add(() -> {
                ZkLock lock = new ZkLock();
                lock.lock();
                for (int j = 0; j < 10; j++) {
                    count++;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.printf("count: %s\n", count);
                lock.unlock();
            });
        }
        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试zk自带的互斥锁
     */
    @Test
    public void testZkMutex() {
        CuratorFramework client = ZkClient.instance.getClient();
        final InterProcessMutex mutex = new InterProcessMutex(client, "/mutex");
        for (int i = 0; i < 10; i++) {
            FutureTaskScheduler.add(() -> {
                try {
                    mutex.acquire();
                    for (int j = 0; j < 10; j++) {
                        count++;
                    }
                    Thread.sleep(1000);
                    System.out.printf("count: %d\n", count);
                    // 释放互斥锁
                    mutex.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
