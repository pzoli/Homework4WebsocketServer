package hu.infokristaly.homework4websocketserver.ws;

import hu.infokristaly.homework4websocketserver.cv.AdvancedMotionDetector;
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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

@Component
public class VideoStreamHandler extends BinaryWebSocketHandler {

    private final static String SOURCE_NAME = "[source]:";

    @Value("${video.path:src/main/resources/videos}")
    private String videoPath;

    @Value("${video.temp.path}")
    private String tempPath;

    @Value("${mqtt.broker}")
    private String broker;

    @Value("${mqtt.topic}")
    private String topic;

    @Value("${mqtt.message}")
    private String content;

    @Value("${video.concat.enabled}")
    private Boolean isConcatFilesEnabled;

    @Value("${video.duration}")
    private Integer videoDuration;

    private static class SessionData {
        public LocalDateTime recordStartTime;
        public boolean isHeaderGrabbed = false;
        public byte[] header;
        private String source;
        private String sessionName;
        private AsyncInputStream asyncStream = null;
        private ExecutorService executor = null;
        private FFmpegFrameGrabber grabber;
        private volatile boolean isRunning = false;
        private AdvancedMotionDetector detector = new AdvancedMotionDetector();
        private LocalDateTime lastMotionDetect = null;
        private MqttClient mqttClient = null;
        private List<String> fileList =  new ArrayList<>();
    }

    private ConcurrentHashMap<String,SessionData> sesssionHolder = new ConcurrentHashMap<>();

    public VideoStreamHandler() {
        System.out.println("VideoStreamHandler created");
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        SessionData sessionData = new SessionData();
        sessionData.recordStartTime = LocalDateTime.now();
        sesssionHolder.put(session.getId(), sessionData);
        sessionData.sessionName = getSessionName(session);
        sessionData.isRunning = true;
        sessionData.asyncStream = new AsyncInputStream();
        System.out.println("VideoStreamHandler connected ["+sessionData.sessionName+"]");
        String fileName = sessionData.sessionName + ".webm";
        sessionData.fileList.add(fileName);
        Files.createFile(Paths.get(tempPath,fileName));

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

                        sessionData.mqttClient = new MqttClient(broker, sessionData.sessionName);
                        MqttConnectOptions connOpts = new MqttConnectOptions();
                        connOpts.setCleanSession(true);
                        System.out.println("Csatlakozás a brokerhez: " + broker);
                        sessionData.mqttClient.connect(connOpts);

                        MqttMessage message = new MqttMessage(("[source:" + sessionData.source + "][" + sessionData.sessionName + "] " + content).getBytes());
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
                                    System.out.println("MQTT Kapcsolat bontva.");
                                }
                            } catch (MqttException e) {
                                System.err.println("MQTT Hiba a bontás során: " + e.getMessage());
                            } finally {
                                try {
                                    // 2. Felszabadítjuk az erőforrásokat (memória, szálak)
                                    // Ezt csak a disconnect után szabad!
                                    sessionData.mqttClient.close();
                                    System.out.println("MQTT Ügyfél véglegesen lezárva.");
                                } catch (MqttException e) {
                                    System.err.println("MQTT Hiba a lezárás során.");
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
                System.out.println("VideoStreamHandler lezárva");
            }
        });
    }

    private String getSessionName(WebSocketSession session) {
        SessionData sessionData = sesssionHolder.get(session.getId());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        Date date = Date.from(sessionData.recordStartTime
                .atZone(ZoneId.systemDefault())
                .toInstant());
        return sdf.format(date) + "_" + session.getId();
    }

    private long totalBytesReceived = 0;
    private long lastLogTime = System.currentTimeMillis();

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws IOException {
        SessionData sessionData = sesssionHolder.get(session.getId());

        java.nio.ByteBuffer payload = message.getPayload();
        int size = payload.remaining();
        byte[] data = new byte[size];
        payload.get(data);

        if (!sessionData.isHeaderGrabbed) {
            sessionData.header = getVideoMHeader(data);
            sessionData.isHeaderGrabbed = true;
        }

        sessionData.asyncStream.write(data);
        Files.write(Paths.get(tempPath,sessionData.sessionName + ".webm"), data, StandardOpenOption.APPEND);

        if (Duration.between(sessionData.recordStartTime,LocalDateTime.now()).toSeconds() >= videoDuration) {
            sessionData.recordStartTime = LocalDateTime.now();
            sessionData.sessionName = getSessionName(session);
            String fileName = sessionData.sessionName + ".webm";
            sessionData.fileList.add(fileName);
            Files.createFile(Paths.get(tempPath,fileName));
            if (!isConcatFilesEnabled) {
                Files.write(Paths.get(tempPath,sessionData.sessionName + ".webm"), sessionData.header, StandardOpenOption.APPEND);
            }
        }

        totalBytesReceived += size;
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime > 5000) {
            System.out.println("Adatfolyam állapota: " + (totalBytesReceived / 1024) + " KB érkezett az utolsó ellenőrzés óta.");
            totalBytesReceived = 0;
            lastLogTime = currentTime;
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        SessionData sessionData = sesssionHolder.get(session.getId());
        String payload = message.getPayload();
        if (sessionData != null && sessionData.isRunning && payload.indexOf(SOURCE_NAME) == 0) {
            sessionData.source = payload.substring(SOURCE_NAME.length());
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
                if (isConcatFilesEnabled) {
                    String fileName = concatFilesJava(sessionId, sessionData);
                    removeFiles(sessionId, sessionData);
                    fixVideoDuration(fileName);
                } else {
                    for(String fileName : sessionData.fileList) {
                        fixVideoDuration(Paths.get(tempPath,fileName).toString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS));
    }

    private void removeFiles(String sessionId,SessionData sessionData) {
        for (String fileName : sessionData.fileList) {
            try {
                Files.delete(Paths.get(tempPath,fileName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void fixVideoDuration(String fileName) throws IOException, InterruptedException {
        String fixedFileName = fileName.replace(".webm", "_fixed.webm");
        int exitCode = repairVideoFile(fileName,fixedFileName);
        if (exitCode == 0) {
            String outFileName = Paths.get(fileName).getFileName().toString();
            Files.move(Paths.get(fixedFileName), Paths.get(videoPath,outFileName), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("A fejrész korrekció megtörtént.");
        } else {;
            File file = new File(fixedFileName);
            if (file.exists()) {
                Files.delete(file.toPath());
            }
            System.out.println("Hiba történt a fejrész korrekciónál! ExitCode: " + exitCode);
        }

    }

    private int repairVideoFile(String inputPath, String outputPath) throws IOException, InterruptedException  {
        ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-y", "-i", inputPath, "-c", "copy", outputPath);
        pb.directory(Paths.get(tempPath).toFile());
        return pb.inheritIO().start().waitFor();
    }

    private String concatFilesJava(String sessionId, SessionData sessionData) throws IOException {
        Path outputPath = Paths.get(tempPath, sessionId+"_concat.webm");

        for (int i = 0; i < sessionData.fileList.size(); i++) {
            Path sourcePath = Paths.get(tempPath, sessionData.fileList.get(i));
            if (i == 0) {
                Files.copy(sourcePath, outputPath, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.write(outputPath, Files.readAllBytes(sourcePath), StandardOpenOption.APPEND);
            }
        }
        return outputPath.toString();
    }

    public byte[] getVideoMHeader(byte[] firstChunk) throws IOException {
        // A WebM/EBML 'Cluster' elem azonosítója: 0x1F 0x43 0xB6 0x75
        byte[] clusterTag = {(byte) 0x1F, (byte) 0x43, (byte) 0xB6, (byte) 0x75};

        int headerLimit = findSequence(firstChunk, clusterTag);

        // Ha nem találjuk a Cluster-t, az egész első chunk-ot fejlécnek tekintjük (biztonsági játék)
        if (headerLimit == -1) {
            headerLimit = firstChunk.length;
        }

        return Arrays.copyOf(firstChunk, headerLimit);
    }

    // Segédfüggvény a bájtsorozat megkereséséhez
    private int findSequence(byte[] data, byte[] sequence) {
        for (int i = 0; i < data.length - sequence.length; i++) {
            boolean match = true;
            for (int j = 0; j < sequence.length; j++) {
                if (data[i + j] != sequence[j]) {
                    match = false;
                    break;
                }
            }
            if (match) return i;
        }
        return -1;
    }
}