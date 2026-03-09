package com.payments;

import com.proto.ProducerProto;
import com.proto.ProducerServiceGrpc;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PaymentsProducer {

    // List of products to randomly pick from
    private static final String[] PRODUCTS = {
            "iPhone 15", "Samsung TV", "MacBook Pro", "Sony Headphones",
            "Nike Shoes", "Levi Jeans", "Kindle", "iPad Mini"
    };

    private static final Random random = new Random();
    private static volatile boolean running = true;
    private static volatile boolean producing = false;

    // Start a simple HTTP server on port 8084
    private static void startControlServer() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8084), 0);

        server.createContext("/start", exchange -> {
            producing = true;
            String response = "{ \"message\": \"Producer started\" }";
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            exchange.getResponseBody().write(response.getBytes());
            exchange.getResponseBody().close();
        });

        server.createContext("/stop", exchange -> {
            producing = false;
            String response = "{ \"message\": \"Producer stopped\" }";
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            exchange.getResponseBody().write(response.getBytes());
            exchange.getResponseBody().close();
        });

        server.start();
        System.out.println("Producer control server started on port 8084");
    }

    public static void main(String[] args) throws Exception {

        startControlServer();

        // Read broker connection details from ENV, fallback to localhost for local dev
        String brokerHost = System.getenv().getOrDefault("BROKER_HOST", "localhost");
        int brokerPort = Integer.parseInt(System.getenv().getOrDefault("BROKER_GRPC_PORT", "9090"));

        // Read events per second from ENV, default to 10
        int eventsPerSecond = Integer.parseInt(System.getenv().getOrDefault("EVENTS_PER_SECOND", "10"));
        long delayMs = 1000L / eventsPerSecond; // e.g. 10 EPS = 100ms delay between events

        System.out.println("Connecting to broker at " + brokerHost + ":" + brokerPort);
        System.out.println("Producing at " + eventsPerSecond + " events/sec");

        // Open gRPC channel to broker
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(brokerHost, brokerPort)
                .usePlaintext()
                .build();

        ProducerServiceGrpc.ProducerServiceStub asyncStub = ProducerServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        // Open stream to broker
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

        // Produce events indefinitely until process is killed
        int eventCount = 0;
        try {
            while (running) {
                if (producing) {
                    eventCount++;
                    ProducerProto.ProducerEvent event = buildEvent(eventCount);
                    eventStream.onNext(event);
                    if (eventCount % 100 == 0) {
                        System.out.println("Produced " + eventCount + " events so far...");
                    }
                    Thread.sleep(delayMs);
                } else {
                    Thread.sleep(500); // idle wait when stopped
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

    private static ProducerProto.ProducerEvent buildEvent(int eventCount) {
        int userId = 1000 + random.nextInt(9000);     // random user between 1000-9999
        int orderId = 100000 + eventCount;             // sequential order ID
        String product = PRODUCTS[random.nextInt(PRODUCTS.length)];
        int amount = 100 + random.nextInt(9900);       // random amount between 100-9999

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