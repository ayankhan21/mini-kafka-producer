package com.payments;

import com.proto.ProducerProto;
import com.proto.ProducerServiceGrpc;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PaymentsProducer {

    private static final String[] PRODUCTS = {
            "iPhone 15", "Samsung TV", "MacBook Pro", "Sony Headphones",
            "Nike Shoes", "Levi Jeans", "Kindle", "iPad Mini"
    };

    private static final Random random = new Random();
    private static volatile boolean running = true;
    private static volatile boolean producing = false;
    private static volatile long currentDelayMs = 10; // default 100 EPS

    private static void startControlServer() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8084), 0);

        // POST /start { "eventsPerSecond": 500, "durationSeconds": 60 }
        server.createContext("/start", exchange -> {
            System.out.println("Received request: " + exchange.getRequestMethod());
            // Handle CORS preflight
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
            exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

            if (exchange.getRequestMethod().equalsIgnoreCase("OPTIONS")) {
                exchange.sendResponseHeaders(204, -1);
                exchange.getResponseBody().close();
                return;
            }
            if (exchange.getRequestMethod().equalsIgnoreCase("POST")) {
                InputStream is = exchange.getRequestBody();
                String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);

                try {
                    int eps = extractInt(body, "eventsPerSecond");
                    int duration = extractInt(body, "durationSeconds");

                    currentDelayMs = 1000L / eps;
                    producing = true;

                    System.out.println("Test started — EPS: " + eps + ", Duration: " + duration + "s");

                    // Auto stop after duration
                    new Thread(() -> {
                        try {
                            Thread.sleep(duration * 1000L);
                            producing = false;
                            System.out.println("Test duration reached, producer stopped.");
                        } catch (InterruptedException ignored) {}
                    }).start();

                    sendResponse(exchange, 200, "{ \"message\": \"Producer started\" }");
                } catch (Exception e) {
                    try {
                        sendResponse(exchange, 400, "{ \"error\": \"" + e.getMessage() + "\" }");
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        });

        // POST /stop
        server.createContext("/stop", exchange -> {
            // Handle CORS preflight
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
            exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

            if (exchange.getRequestMethod().equalsIgnoreCase("OPTIONS")) {
                exchange.sendResponseHeaders(204, -1);
                exchange.getResponseBody().close();
                return;
            }
            producing = false;
            System.out.println("Producer stopped via /stop");
            try {
                sendResponse(exchange, 200, "{ \"message\": \"Producer stopped\" }");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        server.start();
        System.out.println("Producer control server started on port 8084");
    }

    public static void main(String[] args) throws Exception {

        startControlServer();

        String brokerHost = System.getenv().getOrDefault("BROKER_HOST", "localhost");
        int brokerPort = Integer.parseInt(System.getenv().getOrDefault("BROKER_GRPC_PORT", "9090"));

        System.out.println("Connecting to broker at " + brokerHost + ":" + brokerPort);
        System.out.println("Waiting for start signal via POST /start on port 8084...");

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(brokerHost, brokerPort)
                .usePlaintext()
                .build();

        ProducerServiceGrpc.ProducerServiceStub asyncStub = ProducerServiceGrpc.newStub(channel);
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<ProducerProto.ProducerEvent> eventStream = asyncStub.streamEvents(
                new StreamObserver<ProducerProto.Ack>() {
                    @Override
                    public void onNext(ProducerProto.Ack ack) {
                        System.out.println("Broker ack: " + ack.getMessage());
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Stream error: " + t.getMessage());
                        running = false;
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("Stream completed.");
                        latch.countDown();
                    }
                });

        int eventCount = 0;
        try {
            while (running) {
                if (producing) {
                    eventCount++;
                    eventStream.onNext(buildEvent(eventCount));
                    if (eventCount % 1000 == 0) {
                        System.out.println("Produced " + eventCount + " events so far...");
                    }
                    Thread.sleep(currentDelayMs);
                } else {
                    Thread.sleep(500);
                }
            }
        } catch (InterruptedException e) {
            System.out.println("Producer interrupted, shutting down...");
        } finally {
            eventStream.onCompleted();
            latch.await(5, TimeUnit.SECONDS);
            channel.shutdown();
        }
    }

    // Helper to send HTTP response
    private static void sendResponse(com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws Exception {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(status, body.length());
        exchange.getResponseBody().write(body.getBytes());
        exchange.getResponseBody().close();
    }

    // Helper to extract an int field from a simple JSON string
    private static int extractInt(String json, String fieldName) {
        String key = "\"" + fieldName + "\"";
        int keyIndex = json.indexOf(key);
        if (keyIndex == -1) throw new IllegalArgumentException("Missing field: " + fieldName);

        // Move past the key and colon
        String after = json.substring(keyIndex + key.length());

        // Find first digit
        int start = -1;
        for (int i = 0; i < after.length(); i++) {
            if (Character.isDigit(after.charAt(i))) {
                start = i;
                break;
            }
        }
        if (start == -1) throw new IllegalArgumentException("No value for: " + fieldName);

        // Read digits until non-digit
        StringBuilder digits = new StringBuilder();
        for (int i = start; i < after.length(); i++) {
            if (Character.isDigit(after.charAt(i))) {
                digits.append(after.charAt(i));
            } else {
                break;
            }
        }

        return Integer.parseInt(digits.toString());
    }

    private static ProducerProto.ProducerEvent buildEvent(int eventCount) {
        int userId = 1000 + random.nextInt(9000);
        int orderId = 100000 + eventCount;
        String product = PRODUCTS[random.nextInt(PRODUCTS.length)];
        int amount = 100 + random.nextInt(9900);

        String key = "user_" + userId;
        String value = "{"
                + "\"orderId\": \"" + orderId + "\","
                + "\"product\": \"" + product + "\","
                + "\"amount\": " + amount + ","
                + "\"userId\": \"" + userId + "\","
                + "\"event\": \"payment_success\""
                + "}";

        return ProducerProto.ProducerEvent.newBuilder()
                .setKey(key)
                .setValue(value)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }
}