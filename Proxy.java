import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Proxy {

    private static int localPort = 0;
    private static List<InetSocketAddress> targetAddresses = new ArrayList<>();
    private static List<Node> nodes = Collections.synchronizedList(new ArrayList<>());

    private enum Protocol { TCP, UDP }

    private static class Node {
        InetSocketAddress address;
        Protocol protocol;
        List<String> keys;

        Node(InetSocketAddress address, Protocol protocol, List<String> keys) {
            this.address = address;
            this.protocol = protocol;
            this.keys = keys;
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < args.length; ) {
            switch (args[i]) {
                case "-port":
                    if (i + 1 < args.length) {
                        localPort = Integer.parseInt(args[i + 1]);
                        i += 2;
                    } else i++;
                    break;
                case "-server":
                    if (i + 2 < args.length) {
                        String host = args[i + 1];
                        int port = Integer.parseInt(args[i + 2]);
                        targetAddresses.add(new InetSocketAddress(host, port));
                        i += 3;
                    } else i++;
                    break;
                default:
                    i++;
                    break;
            }
        }

        if (localPort == 0 || targetAddresses.isEmpty()) {
            System.err.println("Usage: java Proxy -port <port> -server <address> <port>");
            System.exit(1);
        }

        System.out.println("Proxy configuration loaded. Port: " + localPort);

        startListeners();

        try { Thread.sleep(500); } catch (InterruptedException e) {}

        discoverNodes();

        System.out.println("Proxy is fully ready.");
    }

    private static void discoverNodes() {
        System.out.println("Discovering nodes...");
        for (InetSocketAddress addr : targetAddresses) {
            String discoveryCommand = "GET NAMES visited:" + localPort;
            boolean found = false;

            try (Socket socket = new Socket()) {
                socket.connect(addr, 500);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                out.println(discoveryCommand);
                String response = in.readLine();

                if (response != null && response.startsWith("OK")) {
                    List<String> keys = parseKeys(response);
                    nodes.add(new Node(addr, Protocol.TCP, keys));
                    System.out.println("Connected to " + addr + " via TCP. Keys: " + keys);
                    found = true;
                }
            } catch (IOException e) {
            }

            if (found) continue;

            try (DatagramSocket socket = new DatagramSocket()) {
                socket.setSoTimeout(1000);
                String cmdToSend = discoveryCommand + "\n";
                byte[] buf = cmdToSend.getBytes();
                DatagramPacket packet = new DatagramPacket(buf, buf.length, addr);
                socket.send(packet);

                byte[] recvBuf = new byte[4096];
                DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
                socket.receive(recvPacket);

                String response = new String(recvPacket.getData(), 0, recvPacket.getLength());
                if (response.startsWith("OK")) {
                    List<String> keys = parseKeys(response);
                    nodes.add(new Node(addr, Protocol.UDP, keys));
                    System.out.println("Connected to " + addr + " via UDP. Keys: " + keys);
                }
            } catch (IOException e) {
                System.err.println("Failed to connect to " + addr);
            }
        }
    }

    private static List<String> parseKeys(String response) {
        List<String> keyList = new ArrayList<>();
        String[] parts = response.trim().split("\\s+");
        if (parts.length >= 3) {
            for (int i = 2; i < parts.length; i++) {
                if (!parts[i].startsWith("visited:")) {
                    keyList.add(parts[i]);
                }
            }
        }
        return keyList;
    }

    private static void startListeners() {
        new Thread(() -> startTCPListener()).start();
        new Thread(() -> startUDPListener()).start();
    }

    private static void startTCPListener() {
        try (ServerSocket serverSocket = new ServerSocket(localPort)) {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(() -> handleTCPClient(clientSocket)).start();
                } catch (IOException e) {
                    System.err.println("TCP Accept Error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not start TCP Server: " + e.getMessage());
        }
    }

    private static void startUDPListener() {
        try (DatagramSocket socket = new DatagramSocket(localPort)) {
            while (true) {
                byte[] buffer = new byte[4096];

                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                new Thread(() -> handleUDPClient(socket, packet)).start();
            }
        } catch (IOException e) {
            System.err.println("Could not start UDP Server: " + e.getMessage());
        }
    }

    private static void handleTCPClient(Socket clientSocket) {
        try (
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                Socket socketToClose = clientSocket;
        ) {
            String command = in.readLine();
            if (command != null) {
                System.out.println("[TCP IN] " + command);
                String response = processCommand(command);
                out.println(response);
                System.out.println("[TCP OUT] " + response);
            }
        } catch (IOException e) {
            System.err.println("Error handling TCP client: " + e.getMessage());
        }
    }

    private static void handleUDPClient(DatagramSocket socket, DatagramPacket requestPacket) {
        try {
            String command = new String(requestPacket.getData(), 0, requestPacket.getLength());
            System.out.println("[UDP IN] " + command.trim());

            String response = processCommand(command);

            byte[] responseBytes = response.getBytes();
            DatagramPacket responsePacket = new DatagramPacket(
                    responseBytes, responseBytes.length, requestPacket.getAddress(), requestPacket.getPort()
            );
            socket.send(responsePacket);
            System.out.println("[UDP OUT] " + response.trim());
        } catch (IOException e) {
            System.err.println("Error handling UDP client: " + e.getMessage());
        }
    }

    private static String processCommand(String fullCommand) {
        String cleanCommand = fullCommand.trim();

        String visitedTag = "";
        Set<String> visitedPorts = new HashSet<>();

        String[] tokens = cleanCommand.split("\\s+");
        List<String> commandParts = new ArrayList<>();

        for (String token : tokens) {
            if (token.startsWith("visited:")) {
                visitedTag = token;
                String[] ports = token.substring(8).split(",");
                Collections.addAll(visitedPorts, ports);
            } else {
                commandParts.add(token);
            }
        }

        if (visitedPorts.contains(String.valueOf(localPort))) {
            System.err.println("Cycle detected! Dropping request.");
            if (!commandParts.isEmpty() && commandParts.get(0).equals("GET") && commandParts.size() > 1 && commandParts.get(1).equals("NAMES")) {
                return "OK 0";
            }
            return "NA";
        }

        String newVisitedTag = "visited:" + (visitedTag.length() > 8 ? visitedTag.substring(8) + "," : "") + localPort;

        if (commandParts.isEmpty()) return "NA";
        String cmd = commandParts.get(0);

        if (cmd.equals("GET")) {
            if (commandParts.size() < 2) return "NA";
            String param = commandParts.get(1);

            if (param.equals("NAMES")) {
                Set<String> allKeys = new HashSet<>();

                synchronized (nodes) {
                    for (Node node : nodes) {
                        allKeys.addAll(node.keys);
                    }
                }

                StringBuilder sb = new StringBuilder();
                for (String key : allKeys) {
                    sb.append(" ").append(key);
                }
                return "OK " + allKeys.size() + sb.toString();
            }
            else if (param.equals("VALUE")) {
                if (commandParts.size() < 3) return "NA";
                String keyName = commandParts.get(2);
                return forwardRequest(keyName, "GET VALUE " + keyName + " " + newVisitedTag);
            }
        }
        else if (cmd.equals("SET")) {
            if (commandParts.size() < 3) return "NA";
            String keyName = commandParts.get(1);
            String val = commandParts.get(2);
            return forwardRequest(keyName, "SET " + keyName + " " + val + " " + newVisitedTag);
        }
        else if (cmd.equals("QUIT")) {
            System.exit(0);
        }

        return "NA";
    }

    private static String forwardRequest(String keyName, String commandToSend) {
        synchronized (nodes) {
            for (Node node : nodes) {
                if (node.keys.contains(keyName)) {
                    return sendToNode(node, commandToSend);
                }
            }
        }
        return "NA";
    }

    private static String sendToNode(Node node, String command) {
        if (node.protocol == Protocol.TCP) {
            try (Socket s = new Socket(node.address.getAddress(), node.address.getPort());
                 PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {

                out.println(command);
                return in.readLine();
            } catch (IOException e) {
                return "NA";
            }
        } else {
            try (DatagramSocket s = new DatagramSocket()) {
                s.setSoTimeout(1000);
                String cmdToSend = command.trim() + "\n";
                byte[] buf = cmdToSend.getBytes();
                DatagramPacket pkt = new DatagramPacket(buf, buf.length, node.address);
                s.send(pkt);

                byte[] recvBuf = new byte[4096];
                DatagramPacket recvPkt = new DatagramPacket(recvBuf, recvBuf.length);
                s.receive(recvPkt);
                return new String(recvPkt.getData(), 0, recvPkt.getLength()).trim();
            } catch (IOException e) {
                return "NA";
            }
        }
    }
}