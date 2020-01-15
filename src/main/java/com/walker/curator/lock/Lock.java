package com.walker.curator.lock;

/**
 * <p>
 *
 * </p>
 *
 * @author mu qin
 * @date 2020/1/15
 */
public interface Lock {

    /**
     * 加锁
     *
     * @return
     */
    boolean lock();

    /**
     * 解锁
     *
     * @return
     */
    boolean unlock();
}
