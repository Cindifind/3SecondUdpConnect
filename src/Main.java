import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * UDP 多端口并发探测 + 隧道转发工具
 *
 * 模块划分：
 * - Config.java        : 所有可配置参数
 * - PortPool.java      : 端口池管理（Fisher-Yates 洗牌 + FIFO 队列）
 * - PortManager.java   : 端口管理线程（添加/淘汰端口）
 * - Handshake.java     : 三次握手（PING -> ACK -> ACK_ACK）
 * - Tunnel.java        : 隧道转发（本地监听 + 数据转发 + 心跳 + 空闲超时）
 * - Main.java          : 入口和编排
 */
public class Main {

    // 全局状态
    private static final AtomicBoolean STOP = new AtomicBoolean(false);
    private static final AtomicReference<DatagramSocket> TUNNEL_SOCKET = new AtomicReference<>();
    private static final AtomicReference<InetSocketAddress> TUNNEL_REMOTE = new AtomicReference<>();

    public static void main(String[] args) {
        System.out.println("=== UDP 多端口并发发包 + 隧道转发工具 ===");
        System.out.println("目标主机: " + Config.TARGET_HOST);
        System.out.println("端口池: 1-" + Config.TOTAL_PORTS);
        System.out.println("活跃队列: " + Config.QUEUE_SIZE);
        System.out.println("发包速率: " + Config.RATE_PER_SECOND + " 包/秒/端口");
        System.out.println("本地监听: " + Config.LOCAL_LISTEN_PORT);
        System.out.println("心跳间隔: " + Config.KEEPALIVE_INTERVAL_MS + "ms");
        System.out.println("空闲超时: " + (Config.IDLE_TIMEOUT_MS / 1000) + "秒");
        System.out.println("三次握手: 超时 " + Config.CONFIRM_TIMEOUT_MS + "ms 释放端口\n");

        // 初始化端口池
        PortPool pool = new PortPool();

        // 启动端口管理线程
        Thread managerThread = PortManager.create(pool, STOP, TUNNEL_SOCKET, TUNNEL_REMOTE);
        managerThread.start();

        // 等待隧道建立
        try {
            managerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 启动隧道转发
        if (TUNNEL_SOCKET.get() == null) {
            System.err.println("隧道建立失败，程序退出");
            return;
        }

        Tunnel.start(TUNNEL_SOCKET.get(), TUNNEL_REMOTE.get(), STOP);
    }
}
