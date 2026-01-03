package hu.infokristaly.homework4websocketserver.ws;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncInputStream extends InputStream {
    private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    private final AtomicLong availableBytes = new AtomicLong(0); // Ez a hiányzó változó
    private byte[] currentArray = null;
    private int pointer = 0;
    private boolean closed = false;

    public void write(byte[] data) {
        if (data == null || data.length == 0) return;
        queue.add(data.clone());
        availableBytes.addAndGet(data.length);
    }

    public long getAvailableBytes() {
        return availableBytes.get();
    }

    public void clear() {
        queue.clear();
        currentArray = null;
        pointer = 0;
        availableBytes.set(0);
        System.out.println("Async puffer kiürítve az újraindításhoz.");
    }

    @Override
    public int read() throws IOException {
        if (currentArray == null || pointer >= currentArray.length) {
            if (!fillBuffer()) return -1;
        }
        int b = currentArray[pointer++] & 0xFF;
        availableBytes.decrementAndGet();
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (currentArray == null || pointer >= currentArray.length) {
            if (!fillBuffer()) return -1;
        }
        int available = currentArray.length - pointer;
        int toCopy = Math.min(len, available);
        System.arraycopy(currentArray, pointer, b, off, toCopy);
        pointer += toCopy;
        availableBytes.addAndGet(-toCopy);
        return toCopy;
    }

    private boolean fillBuffer() {
        try {
            if (closed && queue.isEmpty()) return false;
            // A take() helyett poll(), hogy ne fagyjon be a natív szál örökre
            currentArray = queue.poll(1, TimeUnit.SECONDS);
            if (currentArray == null || (closed && currentArray.length == 0)) return false;
            pointer = 0;
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void closeStream() {
        closed = true;
        queue.add(new byte[0]);
    }
}