package main.java.com.rosedb.watch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Watcher {
    private final EventQueue queue;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Watcher(long capacity) {
        this.queue = new EventQueue(capacity);
    }

    public void putEvent(Event event) {
        lock.writeLock().lock();
        try {
            queue.push(event);
            if (queue.isFull()) {
                queue.frontTakeAStep();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Event getEvent() {
        lock.readLock().lock();
        try {
            if (queue.isEmpty()) {
                return null;
            }
            return queue.pop();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void sendEvent(java.util.concurrent.BlockingQueue<Event> channel) {
        while (true) {
            Event event = getEvent();
            if (event == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            try {
                channel.put(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

