import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP→UDP 自动转换模块
 * 监听本地 TCP 端口（如9998），将收到的 TCP/HTTP 请求转为 UDP 发送，
 * 收到 UDP 响应后转回 TCP 回传给客户端。
 */
public class TcpTunnel {

    // 记录每个 TCP 连接对应的输出流，用于回传 UDP 响应
    private static final ConcurrentHashMap<String, OutputStream> TCP_CLIENTS = new ConcurrentHashMap<>();

    // 最后活动时间（共享给空闲检测）
    private static final AtomicLong lastActivityTime;

    static {
        // 通过反射获取 Tunnel 的 lastActivityTime 不方便，直接用独立的
        lastActivityTime = new AtomicLong(System.currentTimeMillis());
    }

    /**
     * 启动 TCP 监听，自动转换为 UDP 转发
     */
    public static Thread start(DatagramSocket tunnel, InetSocketAddress remote, AtomicBoolean stop) {
        Thread tcpThread = new Thread(() -> {
            ServerSocket serverSocket;
            try {
                serverSocket = new ServerSocket(Config.LOCAL_TCP_PORT);
                serverSocket.setReuseAddress(true);
                System.out.println("[TCP] 监听端口 " + Config.LOCAL_TCP_PORT + " 已启动（TCP→UDP 自动转换）");
            } catch (IOException e) {
                System.err.println("[TCP] 无法启动监听: " + e.getMessage());
                return;
            }

            while (!Thread.currentThread().isInterrupted() && !stop.get()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    String clientKey = clientSocket.getInetAddress() + ":" + clientSocket.getPort();
                    System.out.println("[TCP] 新连接: " + clientKey);
                    TCP_CLIENTS.put(clientKey, clientSocket.getOutputStream());

                    // 为每个 TCP 连接启动独立线程处理
                    final Socket cs = clientSocket;
                    final String ck = clientKey;
                    new Thread(() -> handleTcpClient(cs, ck, tunnel, remote), "TCP-" + ck).start();

                } catch (SocketException e) {
                    if (!stop.get()) {
                        System.err.println("[TCP] Accept 异常: " + e.getMessage());
                    }
                } catch (IOException e) {
                    if (!stop.get()) {
                        System.err.println("[TCP] 连接异常: " + e.getMessage());
                    }
                }
            }

            try {
                serverSocket.close();
            } catch (IOException e) {
                // ignore
            }
            System.out.println("[TCP] 监听已停止");
        }, "TcpListener");
        tcpThread.setDaemon(true);
        return tcpThread;
    }

    /**
     * 处理单个 TCP 客户端：读取 TCP 数据 → 转为 UDP 发送
     */
    private static void handleTcpClient(Socket clientSocket, String clientKey,
                                         DatagramSocket tunnel, InetSocketAddress remote) {
        try {
            InputStream in = clientSocket.getInputStream();
            byte[] buf = new byte[Config.TCP_BUFFER_SIZE];

            while (!clientSocket.isClosed() && !Thread.currentThread().isInterrupted()) {
                int len = in.read(buf);
                if (len == -1) {
                    System.out.println("[TCP] 客户端 " + clientKey + " 断开连接");
                    break;
                }

                // TCP 数据 → UDP 转发
                byte[] data = new byte[len];
                System.arraycopy(buf, 0, data, 0, len);
                String content = new String(data);
                System.out.println("[TCP→UDP] " + clientKey + " 收到 " + len + " 字节: " + content);

                DatagramPacket udpPacket = new DatagramPacket(
                        data, data.length, remote.getAddress(), remote.getPort());
                tunnel.send(udpPacket);
                System.out.println("[TCP→UDP] 已转发到 " + remote);
            }

        } catch (IOException e) {
            if (!clientSocket.isClosed()) {
                System.err.println("[TCP] 客户端 " + clientKey + " 异常: " + e.getMessage());
            }
        } finally {
            TCP_CLIENTS.remove(clientKey);
            try {
                clientSocket.close();
            } catch (IOException e) {
                // ignore
            }
            System.out.println("[TCP] 客户端 " + clientKey + " 已关闭");
        }
    }

    /**
     * 当 UDP 隧道收到数据时，转发给所有 TCP 客户端
     * 由 Tunnel 模块调用
     */
    public static void onUdpResponse(byte[] data, int length) {
        String response = new String(data, 0, length);
        for (Map.Entry<String, OutputStream> entry : TCP_CLIENTS.entrySet()) {
            try {
                entry.getValue().write(data, 0, length);
                entry.getValue().flush();
                System.out.println("[UDP→TCP] 已回传给 " + entry.getKey() + ": " + response);
            } catch (IOException e) {
                System.err.println("[UDP→TCP] 回传失败 " + entry.getKey() + ": " + e.getMessage());
                TCP_CLIENTS.remove(entry.getKey());
            }
        }
    }
}
