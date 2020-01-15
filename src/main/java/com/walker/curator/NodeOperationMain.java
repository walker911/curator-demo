package com.walker.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author mu qin
 * @date 2020/1/14
 */
public class NodeOperationMain {

    private static final String ZK_ADDRESS = "127.0.0.1:2181";

    public static void main(String[] args) {
        createNode();
        updateNode();
        updateNodeAsync();
        readNode();
        deleteNode();
    }

    public static void createNode() {
        CuratorFramework client = ClientFactory.createSimple(ZK_ADDRESS);

        try {
            // 启动客户端
            client.start();

            String data = "hello";
            byte[] payload = data.getBytes(StandardCharsets.UTF_8);
            String zkPath = "/test/crud/node-1";
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(zkPath, payload);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    public static void readNode() {
        // 创建客户端
        CuratorFramework client = ClientFactory.createSimple(ZK_ADDRESS);

        try {
            // 启动客户端
            client.start();

            String zkPath = "/test/crud/node-1";

            Stat stat = client.checkExists().forPath(zkPath);
            if (stat != null) {
                // 读取节点数据
                byte[] payload = client.getData().forPath(zkPath);
                String data = new String(payload, StandardCharsets.UTF_8);
                System.out.printf("read data: %s\n", data);

                String parentPath = "/test";
                List<String> children = client.getChildren().forPath(parentPath);
                children.forEach(child -> System.out.printf("child: %s\n", child));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    public static void updateNode() {
        CuratorFramework client = ClientFactory.createSimple(ZK_ADDRESS);

        try {
            client.start();

            String data = "hello world";
            byte[] payload = data.getBytes(StandardCharsets.UTF_8);
            String zkPath = "/test/crud/node-1";
            client.setData().forPath(zkPath, payload);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    public static void updateNodeAsync() {
        CuratorFramework client = ClientFactory.createSimple(ZK_ADDRESS);

        try {
            AsyncCallback.StringCallback callback = (i, s, o, s1) -> System.out.println("i = " + i + "|" +
                    "s = " + s + "|" + "o = " + o + "|" + "s1 = " + s1);
            client.start();

            String data = "hello everybody";
            byte[] payload = data.getBytes(StandardCharsets.UTF_8);
            String zkPath = "/test/crud/node-1";
            client.setData().inBackground(callback).forPath(zkPath, payload);

            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    public static void deleteNode() {
        CuratorFramework client = ClientFactory.createSimple(ZK_ADDRESS);
        try {
            client.start();

            String zkPath = "/test/crud/node-1";
            client.delete().forPath(zkPath);

            String parentPath = "/test";
            List<String> children = client.getChildren().forPath(parentPath);
            children.forEach(child -> System.out.printf("child: %s\n", child));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
