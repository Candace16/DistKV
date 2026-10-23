package com.anjali.distkv.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Interactive command-line client for DistKV.
 *
 * <p>Usage:
 * <pre>
 * java -cp target/classes com.anjali.distkv.cli.DistKvCli [host] [port]
 * distkv> SET key value
 * OK
 * distkv> GET key
 * VALUE value
 * </pre>
 */
public final class DistKvCli {

    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 7000;

        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        } else if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        System.out.printf("Connecting to DistKV at %s:%d...%n", host, port);

        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Connected! Type commands (SET, GET, DEL, EXISTS, TTL, STATS, PING). Type 'exit' to quit.");

            while (true) {
                System.out.print("distkv> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }
                if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                    break;
                }

                out.write(line + "\r\n");
                out.flush();

                String response = in.readLine();
                if (response == null) {
                    System.out.println("[Connection closed by server]");
                    break;
                }
                System.out.println(response);
            }
        } catch (Exception e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }
}
