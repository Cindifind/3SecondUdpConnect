

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Scanner;

public class SendPageack {

    private static final String LOCAL_HOST = "127.0.0.1";
    private static final int LOCAL_PORT = 9999;

    public static void send() throws SocketException {
        Scanner scanner = new Scanner(System.in);
        DatagramSocket socket = new DatagramSocket();
        System.out.println("[UDP客户端] 已启动，输入消息发送到 " + LOCAL_HOST + ":" + LOCAL_PORT);
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