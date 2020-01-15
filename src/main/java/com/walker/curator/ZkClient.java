package com.walker.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;

/**
 * <p>
 *
 * </p>
 *
 * @author mu qin
 * @date 2020/1/15
 */
public class ZkClient {

    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    private CuratorFramework client;
    public static ZkClient instance;

    static {
        instance = new ZkClient();
        instance.init();
    }

    private ZkClient() {
    }

    public void init() {
        if (client != null) {
            return;
        }
        client = ClientFactory.createSimple(ZK_ADDRESS);
        client.start();
    }

    public void destroy() {
        CloseableUtils.closeQuietly(client);
    }

    /**
     * 创建节点
     *
     * @param zkPath
     * @param data
     */
    public void createNode(String zkPath, String data) {
        try {
            byte[] payload = "to set content".getBytes(StandardCharsets.UTF_8);
            if (data != null) {
                payload = data.getBytes(StandardCharsets.UTF_8);
            }
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(zkPath, payload);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点
     *
     * @param zkPath
     */
    public void deleteNode(String zkPath) {
        try {
            if (!isNodeExist(zkPath)) {
                return;
            }
            client.delete().forPath(zkPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 检查节点
     *
     * @param zkPath
     * @return
     */
    public boolean isNodeExist(String zkPath) {
        try {
            Stat stat = client.checkExists().forPath(zkPath);
            if (stat == null) {
                System.out.printf("节点不存在：%s\n", zkPath);
                return false;
            } else {
                System.out.printf("节点存在 stat is: %s\n", zkPath);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建临时顺序节点
     *
     * @param srcPath
     * @return
     */
    public String createEphemeralSeqNode(String srcPath) {
        try {
            String path = client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(srcPath);
            return path;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }
}
