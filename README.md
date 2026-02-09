# Distributed Key-Value Proxy (TCP/UDP) — Java

A small distributed key-value system implemented in Java. Includes TCP/UDP clients and servers, plus a Proxy that discovers nodes and forwards requests to the correct node.

## Components
- TCPServer / UDPServer: serves a single key-value pair (configured via CLI arguments)
- TCPClient / UDPClient: sends commands to a server/proxy
- Proxy: discovers available nodes using `GET NAMES` and forwards requests over TCP or UDP

## Supported Commands
- GET NAMES
- GET VALUE <key>
- SET <key> <value>
- QUIT

## Compile
From the project root:
javac *.java

## How to Run (example)
Start one or more servers (each with its own key), then start the Proxy, then query via client.

Start TCP server:
java TCPServer -port <PORT> -key <KEY> -value <VALUE>

Start UDP server:
java UDPServer -port <PORT> -key <KEY> -value <VALUE>

Start Proxy:
java Proxy -port <LOCAL_PORT> -server <HOST> <PORT> [-server <HOST> <PORT> ...]

Client examples:
java TCPClient -address <HOST> -port <PORT> -command GET NAMES
java TCPClient -address <HOST> -port <PORT> -command GET VALUE <KEY>
java TCPClient -address <HOST> -port <PORT> -command SET <KEY> <VALUE>
