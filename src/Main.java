import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * UDP 多端口发包 + 本地端口隧道转发工具
 * 功能：
 * 1. 维护 1-65535 全端口池，每次随机挑选端口发送 UDP 数据包
 * 2. 活跃端口放入 FIFO 队列，队列满时关闭最老连接并归还端口
 * 3. 可自定义发包速率（每秒包数）和队列长度
 * 4. 任意一个端口收到响应后，立即关闭其他端口的连接
 * 5. 监听本地端口，将本地端口收到的流量通过隧道转发
 */
public class Main {

    // ========== 可配置参数 ==========
    private static final String TARGET_HOST = "113.4.45.38";       // 目标主机 IP
    private static final int RATE_PER_SECOND = 100;               // 每个端口每秒发包数
    private static final byte[] PAYLOAD = "PING".getBytes();     // 发送的数据内容
    private static final int LOCAL_LISTEN_PORT = 9999;           // 本地监听端口
    private static final int QUEUE_SIZE = 3000;                    // 活跃端口队列最大长度（FIFO）
    private static final int PORT_ADD_INTERVAL_MS = 10;          // 每次添加端口的间隔（毫秒）
    private static final int KEEPALIVE_INTERVAL_MS = 30000;       // 隧道建立后心跳间隔（毫秒）
    private static final byte[] KEEPALIVE_PAYLOAD = "KEEPALIVE".getBytes(); // 心跳包内容
    private static final int CONFIRM_TIMEOUT_MS = 3000;            // 三次握手确认超时（毫秒）
    private static final byte[] ACK_PAYLOAD = "ACK".getBytes();           // 第一次确认回复
    private static final byte[] ACK_ACK_PAYLOAD = "ACK_ACK".getBytes();   // 第二次确认回复

    // 全局停止标志：任意端口收到响应后置为 true
    private static final AtomicBoolean STOP = new AtomicBoolean(false);

    // 隧道建立后的活动 socket 和远程地址
    private static final AtomicReference<DatagramSocket> TUNNEL_SOCKET = new AtomicReference<>();
    private static final AtomicReference<InetSocketAddress> TUNNEL_REMOTE = new AtomicReference<>();

    // 记录本地客户端地址 -> 用于回传响应
    private static final Map<String, InetSocketAddress> CLIENT_MAP = new ConcurrentHashMap<>();

    // ========== 端口池与活跃队列 ==========
    // 端口池：打乱的数组，顺序遍历即为随机，O(1) 取端口
    private static final int[] shuffledPorts = new int[65535];
    private static final AtomicInteger shuffleIndex = new AtomicInteger(0);
    // 归还的端口：从队列淘汰后放回此处，优先从这里取
    private static final Set<Integer> returnedPorts = ConcurrentHashMap.newKeySet();
    // 活跃队列：FIFO，按顺序存储正在使用的端口连接
    private static final LinkedBlockingDeque<PortConnection> activeQueue = new LinkedBlockingDeque<>();

    /**
     * 端口连接信息封装
     */
    static class PortConnection {
        final int port;
        final DatagramSocket socket;
        final Thread senderThread;
        final Thread receiverThread;

        PortConnection(int port, DatagramSocket socket, Thread senderThread, Thread receiverThread) {
            this.port = port;
            this.socket = socket;
            this.senderThread = senderThread;
            this.receiverThread = receiverThread;
        }
    }

    public static void main(String[] args) {
        // 初始化端口池 1-65535 并打乱顺序（Fisher-Yates 洗牌）
        for (int i = 0; i < 65535; i++) {
            shuffledPorts[i] = i + 1;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 65535 - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int temp = shuffledPorts[i];
            shuffledPorts[i] = shuffledPorts[j];
            shuffledPorts[j] = temp;
        }

        System.out.println("=== UDP 多端口并发发包 + 隧道转发工具 ===");
        System.out.println("目标主机: " + TARGET_HOST);
        System.out.println("端口池: 1-65535 (共 65535 个端口)");
        System.out.println("活跃队列长度: " + QUEUE_SIZE);
        System.out.println("端口添加间隔: " + PORT_ADD_INTERVAL_MS + " 毫秒");
        System.out.println("发包速率: 每端口 " + RATE_PER_SECOND + " 包/秒");
        System.out.println("本地监听端口: " + LOCAL_LISTEN_PORT);
        System.out.println("监听中... 三次握手确认机制（超时 " + CONFIRM_TIMEOUT_MS + "ms 释放端口）");
        System.out.println("心跳间隔: " + KEEPALIVE_INTERVAL_MS + " 毫秒\n");

        // ========== 端口管理线程 ==========
        Thread managerThread = new Thread(() -> {
            while (!STOP.get()) {
                // 队列未满时，随机选一个端口加入
                if (activeQueue.size() < QUEUE_SIZE) {
                    Integer port = null;
                    // 优先取归还的端口
                    if (!returnedPorts.isEmpty()) {
                        Iterator<Integer> it = returnedPorts.iterator();
                        if (it.hasNext()) {
                            port = it.next();
                            returnedPorts.remove(port);
                        }
                    }
                    // 否则从打乱数组中顺序取（已经是随机顺序）
                    if (port == null) {
                        int idx = shuffleIndex.getAndIncrement();
                        if (idx < 65535) {
                            port = shuffledPorts[idx];
                        }
                    }
                    if (port == null) continue;

                    try {
                        DatagramSocket socket = new DatagramSocket();
                        Thread sender = createSender(port, socket);
                        Thread receiver = createReceiver(port, socket);

                        PortConnection conn = new PortConnection(port, socket, sender, receiver);
                        sender.start();
                        receiver.start();
                        activeQueue.addLast(conn);
                        System.out.println("[管理器] 端口 " + port + " 已加入活跃队列"
                                + " (队列: " + activeQueue.size() + "/" + QUEUE_SIZE
                                + ", 剩余可用: " + (65535 - activeQueue.size()) + ")");
                    } catch (SocketException e) {
                        System.err.println("[管理器] 创建 socket 失败 (端口 " + port + "): " + e.getMessage());
                        returnedPorts.add(port); // 归还端口
                    }
                }

                // 队列满了，关闭最老的连接并归还端口
                while (activeQueue.size() >= QUEUE_SIZE && !STOP.get()) {
                    PortConnection oldest = activeQueue.pollFirst();
                    if (oldest != null) {
                        oldest.socket.close();
                        try {
                            oldest.senderThread.join(2000);
                            oldest.receiverThread.join(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        returnedPorts.add(oldest.port);

                        System.out.println("[管理器] 端口 " + oldest.port + " 已关闭并归还"
                                + " (队列: " + activeQueue.size() + "/" + QUEUE_SIZE
                                + ", 可用池: " + (65535 - activeQueue.size()) + ")");
                    }
                }

                try {
                    Thread.sleep(PORT_ADD_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // STOP 后清理所有活跃连接（跳过隧道 socket）
            PortConnection conn;
            while ((conn = activeQueue.pollFirst()) != null) {
                if (conn.socket != TUNNEL_SOCKET.get() && !conn.socket.isClosed()) {
                    conn.socket.close();
                }
                try {
                    conn.senderThread.join(1000);
                    conn.receiverThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (conn.socket == TUNNEL_SOCKET.get()) {
                    System.out.println("[管理器] 端口 " + conn.port + " 保留为隧道端口");
                } else {
                    System.out.println("[管理器] 端口 " + conn.port + " 已关闭（非隧道端口）");
                }
            }
        }, "PortManager");
        managerThread.setDaemon(true);
        managerThread.start();

        // 等待隧道建立（管理器线程收到响应后 STOP=true，清理完毕退出）
        try {
            managerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ========== 启动本地端口监听，流量通过隧道转发 ==========
        if (TUNNEL_SOCKET.get() == null) {
            System.err.println("隧道建立失败，无法启动本地监听");
            return;
        }

        startLocalTunnel();
    }

    /**
     * 创建发包线程（只负责发送 PING）
     * 三次握手由接收线程处理，发送线程不接收数据
     */
    private static Thread createSender(int port, DatagramSocket socket) {
        return new Thread(() -> {
            InetAddress addr;
            try {
                addr = InetAddress.getByName(TARGET_HOST);
            } catch (UnknownHostException e) {
                System.err.println("[端口 " + port + "] 主机解析失败: " + e.getMessage());
                return;
            }

            long intervalMs = 1000 / RATE_PER_SECOND;

            while (!STOP.get() && !socket.isClosed()) {
                try {
                    long start = System.currentTimeMillis();
                    DatagramPacket packet = new DatagramPacket(
                            PAYLOAD, PAYLOAD.length, addr, port);
                    socket.send(packet);
                    long cost = System.currentTimeMillis() - start;
                    long sleepTime = Math.max(0, intervalMs - cost);
                    Thread.sleep(sleepTime);
                } catch (IOException e) {
                    if (!STOP.get() && !socket.isClosed()) {
                        System.err.println("[端口 " + port + "] 发送异常: " + e.getMessage());
                    }
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Sender-" + port);
    }

    /**
     * 创建接收线程（三次握手确认机制）
     * 流程：
     *   1. 发送方发 PING
     *   2. 接收方收到 PING，回复 ACK
     *   3. 发送方收到 ACK，回复 ACK_ACK
     *   4. 接收方收到 ACK_ACK，隧道建立
     *   5. 任何一步超时3秒，释放端口继续扫描
     */
    private static Thread createReceiver(int port, DatagramSocket socket) {
        return new Thread(() -> {
            byte[] buf = new byte[4096];
            InetAddress addr;
            try {
                addr = InetAddress.getByName(TARGET_HOST);
            } catch (UnknownHostException e) {
                System.err.println("[端口 " + port + "] 主机解析失败: " + e.getMessage());
                return;
            }

            while (!STOP.get() && !socket.isClosed()) {
                try {
                    // === 第1步：等待收到 PING 或 ACK ===
                    socket.setSoTimeout(CONFIRM_TIMEOUT_MS);
                    DatagramPacket recvPacket = new DatagramPacket(buf, buf.length);
                    socket.receive(recvPacket);
                    String response = new String(recvPacket.getData(), 0, recvPacket.getLength());
                    InetSocketAddress senderAddr = new InetSocketAddress(
                            recvPacket.getAddress(), recvPacket.getPort());

                    if ("PING".equals(response)) {
                        // === 第2步：收到 PING，回复 ACK ===
                        System.out.println("[端口 " + port + "] 收到 PING，回复 ACK -> " + senderAddr);
                        DatagramPacket ackPacket = new DatagramPacket(
                                ACK_PAYLOAD, ACK_PAYLOAD.length,
                                senderAddr.getAddress(), senderAddr.getPort());
                        socket.send(ackPacket);

                        // === 第3步：等待收到 ACK_ACK（3秒超时）===
                        System.out.println("[端口 " + port + "] 等待 ACK_ACK...");
                        try {
                            DatagramPacket ackAckPacket = new DatagramPacket(buf, buf.length);
                            socket.receive(ackAckPacket);
                            String ackAckResponse = new String(ackAckPacket.getData(), 0, ackAckPacket.getLength());

                            if ("ACK_ACK".equals(ackAckResponse)) {
                                // === 第4步：收到 ACK_ACK，隧道建立！===
                                if (TUNNEL_SOCKET.compareAndSet(null, socket)) {
                                    // 只有第一个成功设置的才能建立隧道
                                    System.out.println("\n>>> [端口 " + port + "] 三次握手完成! 隧道已建立! <<<");
                                    System.out.println("    远端地址: " + senderAddr);
                                    System.out.println("    握手耗时: <" + CONFIRM_TIMEOUT_MS + "ms");

                                    TUNNEL_REMOTE.set(senderAddr);

                                    System.out.println("    -> 本机发送端口: " + TARGET_HOST + ":" + port);
                                    System.out.println("    -> 远端响应端口: " + senderAddr);
                                    System.out.println("    -> 开始监听本地端口 " + LOCAL_LISTEN_PORT + " 进行转发...\n");

                                    STOP.set(true);
                                } else {
                                    System.out.println("[端口 " + port + "] 三次握手完成，但隧道已由其他端口建立，关闭本端口");
                                }
                                break;
                            } else {
                                System.out.println("[端口 " + port + "] 收到非预期回复: " + ackAckResponse + "，继续等待...");
                            }
                        } catch (SocketTimeoutException e) {
                            // === 第5步：超时，释放端口 ===
                            System.err.println("[端口 " + port + "] 等待 ACK_ACK 超时 (" + CONFIRM_TIMEOUT_MS + "ms)，释放端口");
                            break;
                        }
                    } else if ("ACK".equals(response)) {
                        // 收到 ACK，如果隧道已建立则不回复
                        if (STOP.get()) {
                            System.out.println("[端口 " + port + "] 收到 ACK，但隧道已建立，跳过");
                        } else {
                            // 回复 ACK_ACK 并建立隧道
                            System.out.println("[端口 " + port + "] 收到 ACK，回复 ACK_ACK -> " + senderAddr);
                            DatagramPacket ackAckPacket = new DatagramPacket(
                                    ACK_ACK_PAYLOAD, ACK_ACK_PAYLOAD.length,
                                    senderAddr.getAddress(), senderAddr.getPort());
                            socket.send(ackAckPacket);

                            // 建立隧道（原子操作，只有一个能成功）
                            if (TUNNEL_SOCKET.compareAndSet(null, socket)) {
                                System.out.println("\n>>> [端口 " + port + "] 三次握手完成! 隧道已建立! <<<");
                                System.out.println("    远端地址: " + senderAddr);

                                TUNNEL_REMOTE.set(senderAddr);

                                System.out.println("    -> 本机发送端口: " + TARGET_HOST + ":" + port);
                                System.out.println("    -> 远端响应端口: " + senderAddr);
                                System.out.println("    -> 开始监听本地端口 " + LOCAL_LISTEN_PORT + " 进行转发...\n");

                                STOP.set(true);
                            } else {
                                System.out.println("[端口 " + port + "] 三次握手完成，但隧道已由其他端口建立，关闭本端口");
                            }
                        }
                        break;
                    } else if ("ACK_ACK".equals(response)) {
                        // 收到 ACK_ACK，如果隧道已建立则忽略
                        if (STOP.get()) {
                            System.out.println("[端口 " + port + "] 收到 ACK_ACK，但隧道已建立，忽略");
                        } else {
                            // 建立隧道（原子操作，只有一个能成功）
                            if (TUNNEL_SOCKET.compareAndSet(null, socket)) {
                                System.out.println("\n>>> [端口 " + port + "] 收到 ACK_ACK! 隧道已建立! <<<");
                                System.out.println("    远端地址: " + senderAddr);

                                TUNNEL_REMOTE.set(senderAddr);

                                System.out.println("    -> 本机发送端口: " + TARGET_HOST + ":" + port);
                                System.out.println("    -> 远端响应端口: " + senderAddr);
                                System.out.println("    -> 开始监听本地端口 " + LOCAL_LISTEN_PORT + " 进行转发...\n");

                                STOP.set(true);
                            } else {
                                System.out.println("[端口 " + port + "] 收到 ACK_ACK，但隧道已由其他端口建立，关闭本端口");
                            }
                        }
                        break;
                    } else {
                        // 收到其他响应（可能是旧数据），忽略
                        System.out.println("[端口 " + port + "] 收到未知响应: " + response + "，忽略");
                    }

                } catch (SocketTimeoutException e) {
                    // 超时，继续循环
                } catch (IOException e) {
                    if (!STOP.get() && !socket.isClosed()) {
                        System.err.println("[端口 " + port + "] 接收异常: " + e.getMessage());
                    }
                    break;
                }
            }
        }, "Receiver-" + port);
    }

    /**
     * 监听本地端口，将收到的流量通过已建立的 UDP 隧道转发
     * 同时监听隧道返回的响应，回传给对应的本地客户端
     */
    private static void startLocalTunnel() {
        DatagramSocket localSocket;
        try {
            localSocket = new DatagramSocket(LOCAL_LISTEN_PORT);
            localSocket.setSoTimeout(1000); // 1秒超时，用于检查 STOP 标志
        } catch (SocketException e) {
            System.err.println("无法绑定本地监听端口 " + LOCAL_LISTEN_PORT + ": " + e.getMessage());
            return;
        }

        DatagramSocket tunnel = TUNNEL_SOCKET.get();
        InetSocketAddress remote = TUNNEL_REMOTE.get();

        System.out.println("========================================");
        System.out.println("本地隧道监听已启动!");
        System.out.println("  本地监听: 0.0.0.0:" + LOCAL_LISTEN_PORT);
        System.out.println("  隧道出口: " + remote);
        System.out.println("========================================\n");

        System.out.println("========================================");
        System.out.println("  隧道 socket 本地地址: " + tunnel.getLocalAddress() + ":" + tunnel.getLocalPort());
        System.out.println("  隧道 socket 已绑定: 是");
        System.out.println("  远端目标地址: " + remote);
        System.out.println("========================================\n");

        // 线程1：本地监听 -> 转发到隧道
        Thread localToTunnel = new Thread(() -> {
            byte[] buf = new byte[4096];
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    DatagramPacket localPacket = new DatagramPacket(buf, buf.length);
                    localSocket.receive(localPacket);

                    InetSocketAddress clientAddr = new InetSocketAddress(
                            localPacket.getAddress(), localPacket.getPort());
                    String clientKey = clientAddr.toString();

                    // 记录客户端地址，用于响应回传
                    CLIENT_MAP.put(clientKey, clientAddr);

                    System.out.println("[本地 -> 隧道] 收到来自客户端的数据:");
                    System.out.println("  客户端地址: " + clientKey);
                    System.out.println("  数据长度: " + localPacket.getLength() + " 字节");
                    System.out.println("  本机转发 socket: " + tunnel.getLocalAddress() + ":" + tunnel.getLocalPort());
                    System.out.println("  发送目标地址: " + remote.getAddress() + ":" + remote.getPort());

                    // 转发到隧道远端
                    DatagramPacket tunnelPacket = new DatagramPacket(
                            localPacket.getData(), localPacket.getLength(),
                            remote.getAddress(), remote.getPort());
                    tunnel.send(tunnelPacket);
                    System.out.println("  转发成功!");

                } catch (SocketTimeoutException e) {
                    // 超时，继续循环检查 STOP
                } catch (IOException e) {
                    if (localSocket != null && !localSocket.isClosed()) {
                        System.err.println("[本地监听] 接收异常: " + e.getMessage());
                    }
                    break;
                }
            }
        }, "LocalToTunnel");

        // 线程2：隧道返回 -> 回传给本地客户端
        Thread tunnelToLocal = new Thread(() -> {
            byte[] buf = new byte[4096];
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    DatagramPacket tunnelPacket = new DatagramPacket(buf, buf.length);
                    tunnel.receive(tunnelPacket);

                    String response = new String(tunnelPacket.getData(), 0, tunnelPacket.getLength());
                    System.out.println("[隧道 -> 本地] 收到远端数据:");
                    System.out.println("  数据来源: " + tunnelPacket.getAddress() + ":" + tunnelPacket.getPort());
                    System.out.println("  数据长度: " + tunnelPacket.getLength() + " 字节");
                    System.out.println("  数据内容: " + response);

                    // 如果有本地客户端，回传给它们
                    if (CLIENT_MAP.isEmpty()) {
                        System.out.println("  [警告] 暂无本地客户端连接，数据未转发");
                    } else {
                        for (Map.Entry<String, InetSocketAddress> entry : CLIENT_MAP.entrySet()) {
                            try {
                                DatagramPacket replyPacket = new DatagramPacket(
                                        tunnelPacket.getData(), tunnelPacket.getLength(),
                                        entry.getValue().getAddress(), entry.getValue().getPort());
                                localSocket.send(replyPacket);
                                System.out.println("  已回传给客户端: " + entry.getKey());
                            } catch (IOException ex) {
                                System.err.println("  回传失败: " + ex.getMessage());
                            }
                        }
                    }

                } catch (SocketTimeoutException e) {
                    // 超时，继续循环
                } catch (IOException e) {
                    if (tunnel != null && !tunnel.isClosed()) {
                        System.err.println("[隧道接收] 异常: " + e.getMessage());
                    }
                    break;
                }
            }
        }, "TunnelToLocal");

        // 线程3：心跳保活 -> 定期发送小包维持UDP连接
        Thread keepalive = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && !tunnel.isClosed()) {
                try {
                    DatagramPacket keepalivePacket = new DatagramPacket(
                            KEEPALIVE_PAYLOAD, KEEPALIVE_PAYLOAD.length,
                            remote.getAddress(), remote.getPort());
                    tunnel.send(keepalivePacket);
                    System.out.println("[心跳] 发送保活包: " + tunnel.getLocalAddress() + ":" + tunnel.getLocalPort()
                            + " -> " + remote.getAddress() + ":" + remote.getPort()
                            + " (" + KEEPALIVE_PAYLOAD.length + " 字节)");
                    Thread.sleep(KEEPALIVE_INTERVAL_MS);
                } catch (IOException e) {
                    if (!tunnel.isClosed()) {
                        System.err.println("[心跳] 发送失败: " + e.getMessage());
                    }
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "KeepAlive");

        localToTunnel.setDaemon(true);
        tunnelToLocal.setDaemon(true);
        keepalive.setDaemon(true);
        localToTunnel.start();
        tunnelToLocal.start();
        keepalive.start();

        // 主线程等待
        System.out.println("按 Ctrl+C 停止程序...\n");
        try {
            localToTunnel.join();
            tunnelToLocal.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            localSocket.close();
            if (tunnel != null && !tunnel.isClosed()) {
                tunnel.close();
            }
            System.out.println("\n=== 所有连接已关闭，程序结束 ===");
        }
    }
}
