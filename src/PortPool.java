import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 端口池管理：Fisher-Yates 洗牌 + FIFO 队列
 */
public class PortPool {

    // 打乱的端口数组，顺序遍历即为随机，O(1) 取端口
    private final int[] shuffledPorts = new int[Config.TOTAL_PORTS];
    private final AtomicInteger shuffleIndex = new AtomicInteger(0);

    // 归还的端口：从队列淘汰后放回此处，优先从这里取
    private final Set<Integer> returnedPorts = ConcurrentHashMap.newKeySet();

    // 活跃队列：FIFO，按顺序存储正在使用的端口连接
    private final LinkedBlockingDeque<PortConnection> activeQueue = new LinkedBlockingDeque<>();

    /**
     * 端口连接信息封装
     */
    public static class PortConnection {
        public final int port;
        public final java.net.DatagramSocket socket;
        public final Thread senderThread;
        public final Thread receiverThread;

        public PortConnection(int port, java.net.DatagramSocket socket, Thread senderThread, Thread receiverThread) {
            this.port = port;
            this.socket = socket;
            this.senderThread = senderThread;
            this.receiverThread = receiverThread;
        }
    }

    public PortPool() {
        // Fisher-Yates 洗牌
        for (int i = 0; i < Config.TOTAL_PORTS; i++) {
            shuffledPorts[i] = i + 1;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = Config.TOTAL_PORTS - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int temp = shuffledPorts[i];
            shuffledPorts[i] = shuffledPorts[j];
            shuffledPorts[j] = temp;
        }
    }

    /**
     * 获取下一个可用端口（优先归还的，否则从打乱数组取）
     */
    public Integer nextPort() {
        // 优先取归还的端口
        if (!returnedPorts.isEmpty()) {
            var it = returnedPorts.iterator();
            if (it.hasNext()) {
                Integer port = it.next();
                returnedPorts.remove(port);
                return port;
            }
        }
        // 从打乱数组中顺序取
        int idx = shuffleIndex.getAndIncrement();
        if (idx < Config.TOTAL_PORTS) {
            return shuffledPorts[idx];
        }
        return null;
    }

    /**
     * 归还端口到可用池
     */
    public void returnPort(int port) {
        returnedPorts.add(port);
    }

    /**
     * 获取活跃队列
     */
    public LinkedBlockingDeque<PortConnection> getActiveQueue() {
        return activeQueue;
    }

    /**
     * 队列是否已满
     */
    public boolean isFull() {
        return activeQueue.size() >= Config.QUEUE_SIZE;
    }

    /**
     * 队列当前大小
     */
    public int size() {
        return activeQueue.size();
    }
}
