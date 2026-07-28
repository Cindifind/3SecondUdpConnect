/**
 * 所有可配置参数集中管理
 */
public class Config {
    // ========== 网络参数 ==========
    public static final String TARGET_HOST = "113.4.45.38";       // 目标主机 IP
    public static final int RATE_PER_SECOND = 100;               // 每个端口每秒发包数
    public static final byte[] PAYLOAD = "PING".getBytes();     // 发送的数据内容
    public static final int LOCAL_LISTEN_PORT = 9999;           // 本地监听端口

    // ========== 端口池参数 ==========
    public static final int QUEUE_SIZE = 3000;                    // 活跃端口队列最大长度（FIFO）
    public static final int PORT_ADD_INTERVAL_MS = 10;          // 每次添加端口的间隔（毫秒）
    public static final int TOTAL_PORTS = 65535;                 // 端口总数

    // ========== 三次握手参数 ==========
    public static final int CONFIRM_TIMEOUT_MS = 3000;            // 三次握手确认超时（毫秒）
    public static final byte[] ACK_PAYLOAD = "ACK".getBytes();           // 第一次确认回复
    public static final byte[] ACK_ACK_PAYLOAD = "ACK_ACK".getBytes();   // 第二次确认回复

    // ========== 心跳参数 ==========
    public static final int KEEPALIVE_INTERVAL_MS = 30000;       // 心跳间隔（毫秒）
    public static final byte[] KEEPALIVE_PAYLOAD = "KEEPALIVE".getBytes(); // 心跳包内容

    // ========== 超时参数 ==========
    public static final int IDLE_TIMEOUT_MS = 120_000;           // 隧道建立后空闲超时（2分钟）
}
