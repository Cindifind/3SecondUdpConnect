import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 端口管理线程：负责添加/淘汰端口，维护活跃队列
 */
public class PortManager {

    /**
     * 创建并启动端口管理线程
     */
    public static Thread create(PortPool pool, AtomicBoolean stop,
                                 AtomicReference<DatagramSocket> tunnelSocket,
                                 AtomicReference<InetSocketAddress> tunnelRemote) {
        Thread manager = new Thread(() -> {
            while (!stop.get()) {
                // 队列未满时，添加新端口
                if (!pool.isFull()) {
                    Integer port = pool.nextPort();
                    if (port == null) continue;

                    try {
                        DatagramSocket socket = new DatagramSocket();
                        Thread sender = Handshake.createSender(port, socket, stop);
                        Thread receiver = Handshake.createReceiver(port, socket, stop, tunnelSocket, tunnelRemote);

                        PortPool.PortConnection conn = new PortPool.PortConnection(port, socket, sender, receiver);
                        sender.start();
                        receiver.start();
                        pool.getActiveQueue().addLast(conn);
                        System.out.println("[管理器] 端口 " + port + " 已加入"
                                + " (队列: " + pool.size() + "/" + Config.QUEUE_SIZE
                                + ", 剩余: " + (Config.TOTAL_PORTS - pool.size()) + ")");
                    } catch (SocketException e) {
                        System.err.println("[管理器] 创建 socket 失败 (端口 " + port + "): " + e.getMessage());
                        pool.returnPort(port);
                    }
                }

                // 队列满了，关闭最老的连接
                while (pool.isFull() && !stop.get()) {
                    PortPool.PortConnection oldest = pool.getActiveQueue().pollFirst();
                    if (oldest != null) {
                        oldest.socket.close();
                        try {
                            oldest.senderThread.join(2000);
                            oldest.receiverThread.join(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        pool.returnPort(oldest.port);
                        System.out.println("[管理器] 端口 " + oldest.port + " 已关闭并归还"
                                + " (队列: " + pool.size() + "/" + Config.QUEUE_SIZE + ")");
                    }
                }

                try {
                    Thread.sleep(Config.PORT_ADD_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // STOP 后清理所有活跃连接（跳过隧道 socket）
            PortPool.PortConnection conn;
            while ((conn = pool.getActiveQueue().pollFirst()) != null) {
                if (conn.socket != tunnelSocket.get() && !conn.socket.isClosed()) {
                    conn.socket.close();
                }
                try {
                    conn.senderThread.join(1000);
                    conn.receiverThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (conn.socket == tunnelSocket.get()) {
                    System.out.println("[管理器] 端口 " + conn.port + " 保留为隧道端口");
                } else {
                    System.out.println("[管理器] 端口 " + conn.port + " 已关闭");
                }
            }
        }, "PortManager");
        manager.setDaemon(true);
        return manager;
    }
}
