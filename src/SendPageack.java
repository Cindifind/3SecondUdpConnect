

import java.net.*;
import java.util.Scanner;

public class SendPageack {

    private static final String LOCAL_HOST = "127.0.0.1";
    private static final int LOCAL_PORT = 9999;

    public static void send() throws SocketException {
        Scanner scanner = new Scanner(System.in);
        DatagramSocket socket = new DatagramSocket();
        socket.setSoTimeout(1000);
        System.out.println("[UDP客户端] 已启动，输入消息发送到 " + LOCAL_HOST + ":" + LOCAL_PORT);
        System.out.println("[UDP客户端] 本机地址: " + socket.getLocalAddress() + ":" + socket.getLocalPort());
        System.out.println("[UDP客户端] 等待接收消息...\n");

        // 接收线程：监听远端回复并打印
        Thread receiver = new Thread(() -> {
            byte[] buf = new byte[4096];
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength());
                    System.out.println("[收到] <- " + packet.getAddress() + ":" + packet.getPort() + " : " + msg);
                } catch (SocketTimeoutException e) {
                    // 超时继续
                } catch (Exception e) {
                    if (!socket.isClosed()) {
                        System.err.println("[接收异常] " + e.getMessage());
                    }
                    break;
                }
            }
        }, "Receiver");
        receiver.setDaemon(true);
        receiver.start();

        // 发送线程：从控制台读取并发送
        while (true) {
            try {
                String message = scanner.nextLine();
                byte[] data = message.getBytes();
                InetAddress address = InetAddress.getByName(LOCAL_HOST);
                DatagramPacket packet = new DatagramPacket(data, data.length, address, LOCAL_PORT);
                socket.send(packet);
                System.out.println("[发送] -> " + message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws SocketException {
        send();
    }
}