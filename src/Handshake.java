import java.io.IOException;
import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 三次握手模块：创建发包线程和收包线程
 */
public class Handshake {

    /**
     * 创建发包线程（只负责发送 PING）
     */
    public static Thread createSender(int port, DatagramSocket socket,
                                       AtomicBoolean stop) {
        return new Thread(() -> {
            InetAddress addr;
            try {
                addr = InetAddress.getByName(Config.TARGET_HOST);
            } catch (UnknownHostException e) {
                System.err.println("[端口 " + port + "] 主机解析失败: " + e.getMessage());
                return;
            }

            long intervalMs = 1000 / Config.RATE_PER_SECOND;

            while (!stop.get() && !socket.isClosed()) {
                try {
                    long start = System.currentTimeMillis();
                    DatagramPacket packet = new DatagramPacket(
                            Config.PAYLOAD, Config.PAYLOAD.length, addr, port);
                    socket.send(packet);
                    long cost = System.currentTimeMillis() - start;
                    long sleepTime = Math.max(0, intervalMs - cost);
                    Thread.sleep(sleepTime);
                } catch (IOException e) {
                    if (!stop.get() && !socket.isClosed()) {
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
     */
    public static Thread createReceiver(int port, DatagramSocket socket,
                                         AtomicBoolean stop,
                                         AtomicReference<DatagramSocket> tunnelSocket,
                                         AtomicReference<InetSocketAddress> tunnelRemote) {
        return new Thread(() -> {
            byte[] buf = new byte[4096];

            while (!stop.get() && !socket.isClosed()) {
                try {
                    socket.setSoTimeout(Config.CONFIRM_TIMEOUT_MS);
                    DatagramPacket recvPacket = new DatagramPacket(buf, buf.length);
                    socket.receive(recvPacket);
                    String response = new String(recvPacket.getData(), 0, recvPacket.getLength());
                    InetSocketAddress senderAddr = new InetSocketAddress(
                            recvPacket.getAddress(), recvPacket.getPort());

                    if ("PING".equals(response)) {
                        // 收到 PING，回复 ACK
                        System.out.println("[端口 " + port + "] 收到 PING，回复 ACK -> " + senderAddr);
                        socket.send(new DatagramPacket(
                                Config.ACK_PAYLOAD, Config.ACK_PAYLOAD.length,
                                senderAddr.getAddress(), senderAddr.getPort()));

                        // 等待 ACK_ACK
                        System.out.println("[端口 " + port + "] 等待 ACK_ACK...");
                        try {
                            DatagramPacket ackAckPacket = new DatagramPacket(buf, buf.length);
                            socket.receive(ackAckPacket);
                            String ackAckResponse = new String(ackAckPacket.getData(), 0, ackAckPacket.getLength());

                            if ("ACK_ACK".equals(ackAckResponse)) {
                                if (tunnelSocket.compareAndSet(null, socket)) {
                                    printTunnelEstablished(port, senderAddr, tunnelRemote);
                                    stop.set(true);
                                } else {
                                    System.out.println("[端口 " + port + "] 隧道已由其他端口建立，关闭本端口");
                                }
                                break;
                            } else {
                                System.out.println("[端口 " + port + "] 收到非预期回复: " + ackAckResponse);
                            }
                        } catch (SocketTimeoutException e) {
                            System.err.println("[端口 " + port + "] 等待 ACK_ACK 超时，释放端口");
                            break;
                        }

                    } else if ("ACK".equals(response)) {
                        // 先用原子操作抢占，只有一个能成功
                        if (stop.compareAndSet(false, true)) {
                            // 抢占成功，回复 ACK_ACK 并建立隧道
                            System.out.println("[端口 " + port + "] 收到 ACK，回复 ACK_ACK -> " + senderAddr);
                            socket.send(new DatagramPacket(
                                    Config.ACK_ACK_PAYLOAD, Config.ACK_ACK_PAYLOAD.length,
                                    senderAddr.getAddress(), senderAddr.getPort()));

                            if (tunnelSocket.compareAndSet(null, socket)) {
                                printTunnelEstablished(port, senderAddr, tunnelRemote);
                            }
                        } else {
                            System.out.println("[端口 " + port + "] 收到 ACK，但隧道已建立，跳过");
                        }
                        break;

                    } else if ("ACK_ACK".equals(response)) {
                        // 先用原子操作抢占，只有一个能成功
                        if (stop.compareAndSet(false, true)) {
                            if (tunnelSocket.compareAndSet(null, socket)) {
                                printTunnelEstablished(port, senderAddr, tunnelRemote);
                            }
                        } else {
                            System.out.println("[端口 " + port + "] 收到 ACK_ACK，但隧道已建立，忽略");
                        }
                        break;

                    } else {
                        System.out.println("[端口 " + port + "] 收到未知响应: " + response + "，忽略");
                    }

                } catch (SocketTimeoutException e) {
                    // 超时，继续循环
                } catch (IOException e) {
                    if (!stop.get() && !socket.isClosed()) {
                        System.err.println("[端口 " + port + "] 接收异常: " + e.getMessage());
                    }
                    break;
                }
            }
        }, "Receiver-" + port);
    }

    private static void printTunnelEstablished(int port, InetSocketAddress remote,
                                                AtomicReference<InetSocketAddress> tunnelRemote) {
        tunnelRemote.set(remote);
        System.out.println("\n>>> [端口 " + port + "] 三次握手完成! 隧道已建立! <<<");
        System.out.println("    远端地址: " + remote);
        System.out.println("    -> 本机发送端口: " + Config.TARGET_HOST + ":" + port);
        System.out.println("    -> 远端响应端口: " + remote);
        System.out.println("    -> 开始监听本地端口 " + Config.LOCAL_LISTEN_PORT + " 进行转发...\n");
    }
}
