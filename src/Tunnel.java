import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 隧道转发模块：本地端口监听、数据转发、心跳保活、空闲超时
 */
public class Tunnel {

    // 记录本地客户端地址 -> 用于回传响应
    private static final Map<String, InetSocketAddress> CLIENT_MAP = new ConcurrentHashMap<>();

    // 最后一次收到数据的时间戳（用于空闲超时检测）
    private static final AtomicLong lastActivityTime = new AtomicLong(System.currentTimeMillis());

    /**
     * 启动隧道：本地监听 + 转发 + 心跳 + 空闲超时
     */
    public static void start(DatagramSocket tunnel, InetSocketAddress remote, AtomicBoolean stop) {
        // 重置空闲计时器（从隧道建立时开始计算）
        lastActivityTime.set(System.currentTimeMillis());

        // 给隧道 socket 加超时，确保关闭时线程能立即退出
        try {
            tunnel.setSoTimeout(1000);
        } catch (SocketException e) {
            System.err.println("设置隧道超时失败: " + e.getMessage());
        }

        DatagramSocket localSocket;
        try {
            localSocket = new DatagramSocket(Config.LOCAL_LISTEN_PORT);
            localSocket.setSoTimeout(1000);
        } catch (SocketException e) {
            System.err.println("无法绑定本地监听端口 " + Config.LOCAL_LISTEN_PORT + ": " + e.getMessage());
            return;
        }

        System.out.println("========================================");
        System.out.println("本地隧道监听已启动!");
        System.out.println("  UDP 监听: 0.0.0.0:" + Config.LOCAL_LISTEN_PORT);
        System.out.println("  TCP 监听: 0.0.0.0:" + Config.LOCAL_TCP_PORT + " (TCP→UDP 自动转换)");
        System.out.println("  隧道出口: " + remote);
        System.out.println("  空闲超时: " + (Config.IDLE_TIMEOUT_MS / 1000) + " 秒");
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
                    CLIENT_MAP.put(clientAddr.toString(), clientAddr);
                    lastActivityTime.set(System.currentTimeMillis());

                    System.out.println("[本地 -> 隧道] 客户端 " + clientAddr
                            + " 发送 " + localPacket.getLength() + " 字节");

                    DatagramPacket tunnelPacket = new DatagramPacket(
                            localPacket.getData(), localPacket.getLength(),
                            remote.getAddress(), remote.getPort());
                    tunnel.send(tunnelPacket);

                } catch (SocketTimeoutException e) {
                    // 超时继续
                } catch (IOException e) {
                    if (!localSocket.isClosed()) {
                        System.err.println("[本地监听] 异常: " + e.getMessage());
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

                    lastActivityTime.set(System.currentTimeMillis());
                    String response = new String(tunnelPacket.getData(), 0, tunnelPacket.getLength());
                    System.out.println("[隧道 -> 本地] 收到: " + tunnelPacket.getLength()
                            + " 字节 from " + tunnelPacket.getAddress() + ":" + tunnelPacket.getPort()
                            + " -> " + response);

                    // UDP→TCP 转发：如果有 TCP 客户端连接，回传数据
                    TcpTunnel.onUdpResponse(tunnelPacket.getData(), tunnelPacket.getLength());

                    // 回传给所有已知客户端
                    for (InetSocketAddress client : CLIENT_MAP.values()) {
                        try {
                            DatagramPacket replyPacket = new DatagramPacket(
                                    tunnelPacket.getData(), tunnelPacket.getLength(),
                                    client.getAddress(), client.getPort());
                            localSocket.send(replyPacket);
                            System.out.println("  已回传给 " + client);
                        } catch (IOException ex) {
                            System.err.println("  回传失败: " + ex.getMessage());
                        }
                    }

                } catch (SocketTimeoutException e) {
                    // 超时继续
                } catch (IOException e) {
                    if (!tunnel.isClosed()) {
                        System.err.println("[隧道接收] 异常: " + e.getMessage());
                    }
                    break;
                }
            }
        }, "TunnelToLocal");

        // 线程3：心跳保活
        Thread keepalive = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && !tunnel.isClosed()) {
                try {
                    DatagramPacket keepalivePacket = new DatagramPacket(
                            Config.KEEPALIVE_PAYLOAD, Config.KEEPALIVE_PAYLOAD.length,
                            remote.getAddress(), remote.getPort());
                    tunnel.send(keepalivePacket);
                    System.out.println("[心跳] 保活包 -> " + remote);
                    Thread.sleep(Config.KEEPALIVE_INTERVAL_MS);
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

        // 线程4：空闲超时检测（2分钟无消息则终止）
        Thread idleChecker = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(5000); // 每5秒检查一次
                    long idle = System.currentTimeMillis() - lastActivityTime.get();
                    if (idle > Config.IDLE_TIMEOUT_MS) {
                        System.err.println("\n=== 空闲超时 " + (Config.IDLE_TIMEOUT_MS / 1000)
                                + " 秒，无消息交互，程序终止 ===");
                        stop.set(true);
                        tunnel.close();
                        localSocket.close();
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "IdleChecker");

        localToTunnel.setDaemon(true);
        tunnelToLocal.setDaemon(true);
        keepalive.setDaemon(true);
        idleChecker.setDaemon(true);
        localToTunnel.start();
        tunnelToLocal.start();
        keepalive.start();
        idleChecker.start();

        // 启动 TCP 监听（TCP→UDP 自动转换）
        Thread tcpThread = TcpTunnel.start(tunnel, remote, stop);
        tcpThread.start();

        // 主线程等待
        System.out.println("隧道已就绪，等待数据交互...（" + (Config.IDLE_TIMEOUT_MS / 1000) + "秒无消息自动终止）\n");
        try {
            localToTunnel.join();
            tunnelToLocal.join();
            idleChecker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            localSocket.close();
            if (!tunnel.isClosed()) {
                tunnel.close();
            }
            System.out.println("\n=== 所有连接已关闭，程序结束 ===");
        }
    }
}
