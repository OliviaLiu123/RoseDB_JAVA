package main.java.com.rosedb.watch;

public class EventQueue {
    private final Event[] events;
    private final long capacity;
    private long front;
    private long back;

    public EventQueue(long capacity) {
        this.capacity = capacity;
        this.events = new Event[(int) capacity];
    }

    public void push(Event event) {
        events[(int) back] = event;
        back = (back + 1) % capacity;
    }

    public Event pop() {
        Event event = events[(int) front];
        frontTakeAStep();
        return event;
    }

    public boolean isFull() {
        return (back + 1) % capacity == front;
    }

    public boolean isEmpty() {
        return back == front;
    }

    public void frontTakeAStep() {
        front = (front + 1) % capacity;
    }
}

