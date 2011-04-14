package org.dis.hamcrest.matcher;

public class TimedPeriod {

    private static final long DEFAULT_PAUSE_LENGTH = 500L;

    private long startTime;

    private final long periodLength;

    private final long pauseInMillis;

    public TimedPeriod(final long periodLength) {
        this(periodLength, DEFAULT_PAUSE_LENGTH);
    }

    public TimedPeriod(final long periodLength, final long pauseInMillis) {
        this.periodLength = periodLength;
        this.pauseInMillis = pauseInMillis;
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public boolean expired() {
        return System.currentTimeMillis() > (startTime + periodLength);
    }

    public boolean notExpired() {
        return ! expired();
    }

    public static TimedPeriod within(final long periodLength) {
        return within(periodLength, DEFAULT_PAUSE_LENGTH);
    }

    public static TimedPeriod within(final long periodLength, final long pauseLength) {
        return new TimedPeriod(periodLength, pauseLength);
    }

    public void pause() {
        synchronized (this) {
            try {
                this.wait(pauseInMillis);
            } catch (InterruptedException ignore) {
                //do nothin
            }
        }
    }
}
