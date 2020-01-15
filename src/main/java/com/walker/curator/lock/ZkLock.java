package com.walker.curator.lock;

import com.walker.curator.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *
 * </p>
 *
 * @author mu qin
 * @date 2020/1/15
 */
public class ZkLock implements Lock {

    private static final String ZK_PATH = "/test/lock";
    private static final String LOCK_PREFIX = ZK_PATH + "/";
    private static final long WAIT_TIME = 1000;

    CuratorFramework client;

    private String lockedShortPath = null;
    private String lockedPath = null;
    private String priorPath = null;
    final AtomicInteger lockCount = new AtomicInteger(0);
    private Thread thread;

    public ZkLock() {
        ZkClient.instance.init();
        if (!ZkClient.instance.isNodeExist(ZK_PATH)) {
            ZkClient.instance.createNode(ZK_PATH, null);
        }
        client = ZkClient.instance.getClient();
    }

    /**
     * 加锁的实现
     *
     * @return
     */
    @Override
    public boolean lock() {
        // 可重入，确保同一线程可以重复加锁
        synchronized (this) {
            if (lockCount.get() == 0) {
                thread = Thread.currentThread();
                lockCount.incrementAndGet();
            } else {
                if (!thread.equals(Thread.currentThread())) {
                    return false;
                }
                lockCount.incrementAndGet();
                return true;
            }
        }

        try {
            // 首先尝试加锁
            boolean locked = tryLock();

            if (locked) {
                return true;
            }

            // 如果加锁失败就去等待
            while (!locked) {
                // 等待
                await();
                // 获取等待的子节点列表
                List<String> waiters = getWaiters();
                // 判断是否加锁成功
                if (checkLocked(waiters)) {
                    locked = true;
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            unlock();
        }

        return false;
    }

    @Override
    public boolean unlock() {
        // 只有加锁的线程能够解锁
        if (!thread.equals(Thread.currentThread())) {
            return false;
        }
        // 减少可重入的计数
        int newLockCount = lockCount.decrementAndGet();
        // 不能小于0
        if (newLockCount < 0) {
            throw new IllegalMonitorStateException("计数不对：" + lockedPath);
        }
        if (newLockCount != 0) {
            return true;
        }
        try {
            // 删除临时节点
            if (ZkClient.instance.isNodeExist(lockedPath)) {
                client.delete().forPath(lockedPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean tryLock() throws Exception {
        // 创建临时ZNode节点
        lockedPath = ZkClient.instance.createEphemeralSeqNode(LOCK_PREFIX);
        if (null == lockedPath) {
            throw new Exception("zk error");
        }
        // 取得加锁的排队编号
        lockedShortPath = getShortPath(lockedPath);
        // 获取加锁的队列
        List<String> waiters = getWaiters();
        // 获取等待的子列表，判断自己是否第一个
        if (checkLocked(waiters)) {
            return true;
        }
        // 判断自己排第几个
        int index = Collections.binarySearch(waiters, lockedShortPath);
        if (index < 0) {
            // 网络抖动，获取到的子节点列表里可能已经没有自己了
            throw new Exception("节点没有找到：" + lockedShortPath);
        }
        // 如果自己没有获得锁，保存前一个节点，稍后会监听前一个节点
        priorPath = ZK_PATH + "/" + waiters.get(index - 1);
        return false;
    }

    /**
     * 等待
     * 监听前一个节点的删除事件
     *
     * @throws Exception
     */
    private void await() throws Exception {
        if (null == priorPath) {
            throw new Exception("prior path error");
        }
        final CountDownLatch latch = new CountDownLatch(1);
        // 监听方式一：Watcher一次性订阅
        Watcher watcher = event -> {
            System.out.printf("监听到的变化watchedEvent=%s\n", event);
            System.out.println("[WatchedEvent]节点删除");
            latch.countDown();
        };
        // 监听方式二：TreeCache订阅
        // treeCacheListener(latch);

        // 开始监听
        client.getData().usingWatcher(watcher).forPath(priorPath);
        latch.await(WAIT_TIME, TimeUnit.SECONDS);
    }

    /**
     * 从zookeeper中拿到所有等待节点
     *
     * @return
     */
    private List<String> getWaiters() {
        List<String> children;
        try {
            children = client.getChildren().forPath(ZK_PATH);
        } catch (Exception e) {
            e.printStackTrace();
            children = new ArrayList<>();
        }
        return children;
    }

    private boolean checkLocked(List<String> waiters) {
        // 节点按编号排序
        Collections.sort(waiters);
        // 如果是第一个，代表自己已经获得了锁
        if (lockedShortPath.equals(waiters.get(0))) {
            System.out.printf("成功地获取到分布式锁，节点为：%s\n", lockedShortPath);
            return true;
        }
        return false;
    }

    /**
     * 获取路径编号
     *
     * @param lockedPath
     * @return
     */
    private String getShortPath(String lockedPath) {
        int index = lockedPath.lastIndexOf(ZK_PATH + "/");
        if (index >= 0) {
            index += ZK_PATH.length() + 1;
            return index <= lockedPath.length() ? lockedPath.substring(index) : "";
        }
        return null;
    }

    /**
     * 监听方式二
     *
     * @param latch
     * @throws Exception
     */
    private void treeCacheListener(CountDownLatch latch) throws Exception {
        TreeCache treeCache = new TreeCache(client, priorPath);
        TreeCacheListener listener = (client, event) -> {
            ChildData data = event.getData();
            if (data != null) {
                if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED) {
                    System.out.printf("[TreeCache]节点删除，path=%s，data=%s\n", data.getPath(), Arrays.toString(data.getData()));
                    latch.countDown();
                }
            }
        };
        treeCache.getListenable().addListener(listener);
        treeCache.start();
    }
}
