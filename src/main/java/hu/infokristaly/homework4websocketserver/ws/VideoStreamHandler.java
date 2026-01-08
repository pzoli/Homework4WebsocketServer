package hu.infokristaly.homework4websocketserver.ws;

import hu.infokristaly.homework4websocketserver.cv.AdvancedMotionDetector;
import jakarta.websocket.CloseReason;
import org.bytedeco.javacv.*;
import org.bytedeco.opencv.opencv_core.Mat;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.*;

@Component
public class VideoStreamHandler extends BinaryWebSocketHandler {

    @Value("${video.path:src/main/resources/videos}")
    private String videoPath;

    @Value("${mqtt.broker}")
    private String broker;

    @Value("${mqtt.topic}")
    private String topic;

    @Value("${mqtt.message}")
    private String content;

    private class SessionData {
        private AsyncInputStream asyncStream = null;
        private ExecutorService executor = null;
        private FFmpegFrameGrabber grabber;
        private volatile boolean isRunning = false;
        private AdvancedMotionDetector detector = new AdvancedMotionDetector();
        private LocalDateTime lastMotionDetect = null;
        private MqttClient mqttClient = null;
    }

    private ConcurrentHashMap<String,SessionData> sesssionHolder = new ConcurrentHashMap<>();

    public VideoStreamHandler() {
        System.out.println("VideoStreamHandler created");
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        SessionData sessionData = new SessionData();
        sesssionHolder.put(session.getId(), sessionData);
        sessionData.isRunning = true;
        sessionData.asyncStream = new AsyncInputStream();
        System.out.println("VideoStreamHandler connected ["+session.getId()+"]");
        Files.createFile(Paths.get(videoPath,session.getId()+".webm"));
        sessionData.executor = Executors.newSingleThreadExecutor();

        sessionData.executor.submit(() -> {
            try {
                while (sessionData.isRunning) {
                    try {
                        sessionData.asyncStream.clear();

                        System.out.println("Várakozás tiszta fejlécre...");
                        while (sessionData.isRunning && sessionData.asyncStream.getAvailableBytes() < 1024 * 1024) {
                            Thread.sleep(100);
                        }

                        sessionData.grabber = new FFmpegFrameGrabber(sessionData.asyncStream);
                        sessionData.grabber.setVideoCodecName("vp8");
                        sessionData.grabber.setFormat("webm");

                        sessionData.grabber.setOption("fflags", "nobuffer+igndts");
                        sessionData.grabber.setOption("probesize", "1048576");
                        System.out.println("FFmpeg indítása...");
                        sessionData.grabber.start(false);
                        System.out.println("FFmpeg sikeresen elindult.");

                        sessionData.mqttClient = new MqttClient(broker, session.getId());
                        MqttConnectOptions connOpts = new MqttConnectOptions();
                        connOpts.setCleanSession(true);
                        System.out.println("Csatlakozás a brokerhez: " + broker);
                        sessionData.mqttClient.connect(connOpts);

                        MqttMessage message = new MqttMessage(("[" + session.getId() + "] " + content).getBytes());
                        message.setQos(2);

                        OpenCVFrameConverter.ToMat converter = new OpenCVFrameConverter.ToMat();
                        int nullFrameCount = 0;
                        while (sessionData.isRunning) {
                            try {
                                Frame frame = sessionData.grabber.grabImage();

                                if (frame == null) {
                                    nullFrameCount++;
                                    if (nullFrameCount > 20) {
                                        System.out.println("A stream megszakadt vagy elfogyott az adat. Újraindítás...");
                                        break;
                                    }
                                    Thread.sleep(500);
                                    continue;
                                }

                                nullFrameCount = 0;

                                if (frame.image != null) {
                                    Mat mat = converter.convert(frame);
                                    if (mat != null && !mat.empty()) {
                                        LocalDateTime end = LocalDateTime.now();
                                        if (sessionData.lastMotionDetect == null) {
                                            sessionData.mqttClient.publish(topic, message);
                                            System.out.println("Mozgás észlelve!");
                                            sessionData.lastMotionDetect = end;
                                        } else {
                                            Duration duration = Duration.between(sessionData.lastMotionDetect, end);
                                            long toSeconds = duration.toSeconds();
                                            if (sessionData.detector.detectMotionFromMat(mat) && (toSeconds > 15)) {
                                                sessionData.mqttClient.publish(topic, message);
                                                System.out.println("Mozgás észlelve!");
                                                sessionData.lastMotionDetect = end;
                                            }
                                        }
                                        mat.release();
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println("Hiba a frame olvasása közben: " + e.getMessage());
                                break; // Hiba esetén is újraindítunk
                            } finally {

                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Grabber indítási hiba: " + e.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException i) {
                            break;
                        }
                    } finally {
                        if (sessionData.grabber != null) {
                            sessionData.grabber.close();
                            sessionData.grabber.release();
                        }
                        if (sessionData.mqttClient != null) {
                            try {
                                if (sessionData.mqttClient.isConnected()) {
                                    // 1. Megszüntetjük a hálózati kapcsolatot (időtúllépéssel, hogy ne akadjon el)
                                    sessionData.mqttClient.disconnect(5000);
                                    System.out.println("Kapcsolat bontva.");
                                }
                            } catch (MqttException e) {
                                System.err.println("Hiba a bontás során: " + e.getMessage());
                            } finally {
                                try {
                                    // 2. Felszabadítjuk az erőforrásokat (memória, szálak)
                                    // Ezt csak a disconnect után szabad!
                                    sessionData.mqttClient.close();
                                    System.out.println("Ügyfél véglegesen lezárva.");
                                } catch (MqttException e) {
                                    System.err.println("Hiba a lezárás során.");
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
                System.out.println("VideoStreamHandler closed");
            }
        });
        //new Thread(this::simulateUpload).start();
    }

    private long totalBytesReceived = 0;
    private long lastLogTime = System.currentTimeMillis();

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws IOException {
        SessionData sessionData = sesssionHolder.get(session.getId());
        // A WebSocket szál csak ír, sosem vár!
        java.nio.ByteBuffer payload = message.getPayload();
        int size = payload.remaining();
        byte[] data = new byte[size];
        payload.get(data);

        sessionData.asyncStream.write(data);
        Files.write(Paths.get(videoPath,session.getId()+".webm"), data, StandardOpenOption.APPEND);

        totalBytesReceived += size;
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime > 5000) {
            System.out.println("Adatfolyam állapota: " + (totalBytesReceived / 1024) + " KB érkezett az utolsó ellenőrzés óta.");
            totalBytesReceived = 0;
            lastLogTime = currentTime;
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        SessionData sessionData = sesssionHolder.get(session.getId());
        String sessionId = session.getId();
        sessionData.isRunning = false;
        if (sessionData.asyncStream != null) {
            sessionData.asyncStream.closeStream();
        }
        sessionData.executor.shutdownNow();
        sesssionHolder.remove(sessionId);
        System.out.println("Kapcsolat lezárva, erőforrások felszabadítva.");

        CompletableFuture.runAsync(() -> {
            try {
                fixVideoDuration(sessionId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS));

    }

    private void fixVideoDuration(String sesionId) throws IOException, InterruptedException {
        Path inputPath = Paths.get(sesionId+".webm");
        String outputPath = inputPath.toString().replace(".webm", "_fixed.webm");
        int exitCode = repairVideoFile(inputPath.toString(),outputPath);
        if (exitCode == 0) {
            Files.move(Paths.get(videoPath,outputPath), Paths.get(videoPath,inputPath.toString()), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("A fejrész korrekció megtörtént.");
        } else {
            Files.delete(Paths.get(videoPath, outputPath));
            System.out.println("Hiba történt a fejrész korrekciónál! ExitCode: " + exitCode);
        }

    }

    private int repairVideoFile(String inputPath, String outputPath) throws IOException, InterruptedException  {
        ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-y", "-i", inputPath, "-c", "copy", outputPath);
        pb.directory(Paths.get(videoPath).toFile());
        return pb.inheritIO().start().waitFor();
    }
}