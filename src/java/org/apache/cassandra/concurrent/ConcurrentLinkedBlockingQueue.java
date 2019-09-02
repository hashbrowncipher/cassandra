package org.apache.cassandra.concurrent;

import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLinkedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private final AtomicInteger freeSlots;
    private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<>();
    private final WaitQueue hasRoom = new WaitQueue();
    private final WaitQueue hasItems = new WaitQueue();

    public ConcurrentLinkedBlockingQueue() {
        this(Integer.MAX_VALUE);
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return !queue.isEmpty();
            }

            @Override
            public E next() {
                return poll();
            }
        };
    }

    @Override
    public int size() {
        return queue.size();
    }

    public ConcurrentLinkedBlockingQueue(int limit) {
        this.freeSlots = new AtomicInteger(limit);
    }

    private E innerPoll() {
        E result = queue.poll();
        if(result != null) {
            freeSlots.incrementAndGet();
            hasRoom.signal();
        }
        return result;
    }

    private void innerPut(E item) {
        queue.offer(item);
        hasItems.signal();
    }

    private E innerTake(long l, TimeUnit timeUnit) throws InterruptedException {
        E ret;
        while(true) {
            ret = innerPoll();
            if(ret != null) {
                break;
            }

            WaitQueue.Signal s = hasItems.register();
            ret = innerPoll();
            if (ret != null) {
                s.cancel();
                break;
            }

            if(l == 0) {
                s.await();
            } else {
                s.awaitTimeout(l, timeUnit);
            }
        }
        return ret;
    }

    public E poll() {
        return innerPoll();
    }

    @Override
    public E peek() {
        return queue.peek();
    }

    public boolean offer(E item) {
        if(freeSlots.get() <= 0) {
            return false;
        }

        // Yes, this absolutely could result in freeSlots becoming negative.
        // It is a race condition that means that this queue cannot ensure boundedness exactly.
        freeSlots.decrementAndGet();
        innerPut(item);

        return true;
    }

    @Override
    public void put(E item) {
        int surplus = freeSlots.decrementAndGet();
        innerPut(item);

        if (surplus < 0) {
            WaitQueue.Signal s = hasRoom.register();
            if (freeSlots.get() < 0) {
                s.awaitUninterruptibly();
            } else {
                s.cancel();
            }
        }
    }

    @Override
    public boolean offer(E item, long l, TimeUnit timeUnit) throws InterruptedException {
        int surplus = freeSlots.decrementAndGet();

        if (surplus < 0) {
            WaitQueue.Signal s = hasRoom.register();
            if (freeSlots.get() < 0) {
                if(!s.awaitTimeout(l, timeUnit)) {
                    freeSlots.incrementAndGet();
                    return false;
                }
            } else {
                s.cancel();
            }
        }

        innerPut(item);
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        return innerTake(0, TimeUnit.SECONDS);
    }

    @Override
    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        return innerTake(l, timeUnit);
    }

    @Override
    public int remainingCapacity() {
        return freeSlots.get();
    }

    @Override
    public int drainTo(Collection<? super E> collection) {
        return drainTo(collection, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> collection, int maxElements) {
        int i = 0;
        for(; i < maxElements; i++) {
            E item = poll();
            if(item == null) {
                break;
            }
            collection.add(item);
        }
        return i;
    }
}
