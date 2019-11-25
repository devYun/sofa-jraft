/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alipay.sofa.jraft.util;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Light-weight object pool based on a thread-local stack.
 * <p/>
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @param <T> the type of the pooled object
 */
public abstract class Recyclers<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Recyclers.class);
    //id生成器
    private static final AtomicInteger idGenerator = new AtomicInteger(Integer.MIN_VALUE);

    private static final int OWN_THREAD_ID = idGenerator.getAndIncrement();
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024; // Use 4k instances as default.
    private static final int DEFAULT_MAX_CAPACITY_PER_THREAD;
    private static final int INITIAL_CAPACITY;

    static {
        // 每个线程的最大对象池容量
        int maxCapacityPerThread = SystemPropertyUtil.getInt("jraft.recyclers.maxCapacityPerThread", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD);
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;
        if (LOG.isDebugEnabled()) {
            if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
                LOG.debug("-Djraft.recyclers.maxCapacityPerThread: disabled");
            } else {
                LOG.debug("-Djraft.recyclers.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
            }
        }
        // 设置初始化容量信息
        INITIAL_CAPACITY = Math.min(DEFAULT_MAX_CAPACITY_PER_THREAD, 256);
    }

    public static final Handle NOOP_HANDLE = new Handle() {};

    private final int maxCapacityPerThread;
    private final ThreadLocal<Stack<T>> threadLocal = new ThreadLocal<Stack<T>>() {

        @Override
        protected Stack<T> initialValue() {
            return new Stack<>(Recyclers.this, Thread.currentThread(), maxCapacityPerThread);
        }
    };

    protected Recyclers() {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD);
    }

    protected Recyclers(int maxCapacityPerThread) {
        this.maxCapacityPerThread = Math.max(0, maxCapacityPerThread);
    }

    @SuppressWarnings("unchecked")
    public final T get() {
        //如果maxCapacityPerThread == 0，禁止回收功能
        //创建一个对象，其Recycler.Handle<User> handle属性为NOOP_HANDLE，
        //该对象的recycle(Object object)不做任何事情，即不做回收
        if (maxCapacityPerThread == 0) {
            return newObject(NOOP_HANDLE);
        }
        //从threadLocal中获取一个栈对象
        Stack<T> stack = threadLocal.get();
        //拿出栈顶元素
        DefaultHandle handle = stack.pop();
        //如果栈里面没有元素，那么就实例化一个
        if (handle == null) {
            handle = stack.newHandle();
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    public final boolean recycle(T o, Handle handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }
        DefaultHandle h = (DefaultHandle) handle;

        //TODO 从netty抄过来的，因为stack可能为空
        //if (h.lastRecycledId != h.recycleId || h.stack == null) {
        //    throw new IllegalStateException("recycled already");
        //}

        //stack在实例化的时候会在构造器中传入一个Recyclers作为parent
        //所以这里是校验一下，如果不是从当前实例申请的资源，那么就不回收
        if (h.stack.parent != this) {
            return false;
        }
        if (o != h.value) {
            throw new IllegalArgumentException("o does not belong to handle");
        }
        h.recycle();
        return true;
    }

    protected abstract T newObject(Handle handle);

    public final int threadLocalCapacity() {
        return threadLocal.get().elements.length;
    }

    public final int threadLocalSize() {
        return threadLocal.get().size;
    }

    public interface Handle {}

    static final class DefaultHandle implements Handle {
        //在WeakOrderQueue的add方法中会设置成ID
        //在push方法中设置成为OWN_THREAD_ID
        //在pop方法中设置为0
        private int lastRecycledId;
        //只有在push方法中才会设置OWN_THREAD_ID
        //在pop方法中设置为0
        private int recycleId;
        //当前的DefaultHandle对象所属的Stack
        private Stack<?> stack;
        private Object value;

        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        public void recycle() {
            Thread thread = Thread.currentThread();
            //如果当前线程正好等于stack所对应的线程，那么直接push进去
            if (thread == stack.thread) {
                stack.push(this);
                return;
            }
            // we don't want to have a ref to the queue as the value in our weak map
            // so we null it out; to ensure there are no races with restoring it later
            // we impose a memory ordering here (no-op on x86)
            // 如果不是当前线程，则需要延迟回收，获取当前线程存储的延迟回收WeakHashMap
            Map<Stack<?>, WeakOrderQueue> delayedRecycled = Recyclers.delayedRecycled.get();
            // 当前 handler 所在的 stack 是否已经在延迟回收的任务队列中
            // 并且 WeakOrderQueue是一个多线程间可以共享的Queue
            WeakOrderQueue queue = delayedRecycled.get(stack);
            if (queue == null) {
                delayedRecycled.put(stack, queue = new WeakOrderQueue(stack, thread));
            }
            queue.add(this);
        }
    }

    private static final ThreadLocal<Map<Stack<?>, WeakOrderQueue>> delayedRecycled = ThreadLocal.withInitial(WeakHashMap::new);

    // a queue that makes only moderate guarantees about visibility: items are seen in the correct order,
    // but we aren't absolutely guaranteed to ever see anything at all, thereby keeping the queue cheap to maintain
    private static final class WeakOrderQueue {
        private static final int LINK_CAPACITY = 16;

        // Let Link extend AtomicInteger for intrinsics. The Link itself will be used as writerIndex.
        @SuppressWarnings("serial")
        private static final class Link extends AtomicInteger {
            private final DefaultHandle[] elements = new DefaultHandle[LINK_CAPACITY];

            private int readIndex;
            private Link next;
        }

        // chain of data items
        private Link head, tail;
        // pointer to another queue of delayed items for the same stack
        private WeakOrderQueue next;
        // 将持有该Queue的对象进行设置为虚引用，就可以通过 null 判断对该线程的资源进行回收
        private final WeakReference<Thread> owner;
        // WeakOrderQueue的唯一标记
        private final int id = idGenerator.getAndIncrement();

        WeakOrderQueue(Stack<?> stack, Thread thread) {
            head = tail = new Link();
            //使用的是WeakReference ，作用是在poll的时候，如果owner不存在了
            // 则需要将该线程所包含的WeakOrderQueue的元素释放，然后从链表中删除该Queue。
            owner = new WeakReference<>(thread);
            //假设线程B和线程C同时回收线程A的对象时，有可能会同时创建一个WeakOrderQueue，就坑同时设置head，所以这里需要加锁
            synchronized (stackLock(stack)) {
                next = stack.head;
                stack.head = this;
            }
        }

        private Object stackLock(Stack<?> stack) {
            return stack;
        }

        void add(DefaultHandle handle) {
            // 设置handler的最近一次回收的id信息，标记此时暂存的handler是被谁回收的
            handle.lastRecycledId = id;

            Link tail = this.tail;
            int writeIndex;
            // 判断一个Link对象是否已经满了：
            // 如果没满，直接添加；
            // 如果已经满了，创建一个新的Link对象，之后重组Link链表，然后添加元素的末尾的Link（除了这个Link，前边的Link全部已经满了）
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                this.tail = tail = tail.next = new Link();
                writeIndex = tail.get();
            }
            tail.elements[writeIndex] = handle;
            // 如果使用者在将DefaultHandle对象压入队列后，将Stack设置为null
            // 但是此处的DefaultHandle是持有stack的强引用的，则Stack对象无法回收；
            //而且由于此处DefaultHandle是持有stack的强引用，WeakHashMap中对应stack的WeakOrderQueue也无法被回收掉了，导致内存泄漏
            handle.stack = null;
            // we lazy set to ensure that setting stack to null appears before we unnull it in the owning thread;
            // this also means we guarantee visibility of an element in the queue if we see the index updated
            // tail本身继承于AtomicInteger，所以此处直接对tail进行+1操作
            tail.lazySet(writeIndex + 1);
        }

        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        // transfer as many items as we can from this queue to the stack, returning true if any were transferred
         // 进行数据的转移，将 Queue 中的暂存的对象移到 Stack 中
        @SuppressWarnings("rawtypes")
        boolean transfer(Stack<?> dst) {
            //寻找第一个Link
            Link head = this.head;
            // head == null，没有存储数据的节点，直接返回
            if (head == null) {
                return false;
            }
            // 读指针的位置已经到达了每个 Node 的存储容量，如果还有下一个节点，进行节点转移
            if (head.readIndex == LINK_CAPACITY) {
                //判断当前的Link节点的下一个节点是否为null，如果为null，说明已经达到了Link链表尾部，直接返回，
                if (head.next == null) {
                    return false;
                }
                // 否则，将当前的Link节点的下一个Link节点赋值给head和this.head.link，进而对下一个Link节点进行操作
                this.head = head = head.next;
            }
            // 获取Link节点的readIndex,即当前的Link节点的第一个有效元素的位置
            final int srcStart = head.readIndex;
            // 获取Link节点的writeIndex，即当前的Link节点的最后一个有效元素的位置
            int srcEnd = head.get();
            // 本次可转移的对象数量（写指针减去读指针）
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }
            // 获取转移元素的目的地Stack中当前的元素个数
            final int dstSize = dst.size;
            // 计算期盼的容量
            final int expectedCapacity = dstSize + srcSize;
            // 期望的容量大小与实际 Stack 所能承载的容量大小进行比对，取最小值
            if (expectedCapacity > dst.elements.length) {
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                srcEnd = Math.min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            if (srcStart != srcEnd) {
                // 获取Link节点的DefaultHandle[]
                final DefaultHandle[] srcElems = head.elements;
                // 获取目的地Stack的DefaultHandle[]
                final DefaultHandle[] dstElems = dst.elements;
                // dst数组的大小，会随着元素的迁入而增加，如果最后发现没有增加，那么表示没有迁移成功任何一个元素
                int newDstSize = dstSize;
                //// 进行对象转移
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle element = srcElems[i];
                    // 表明自己还没有被任何一个 Stack 所回收
                    if (element.recycleId == 0) {
                        element.recycleId = element.lastRecycledId;
                    //  避免对象重复回收
                    } else if (element.recycleId != element.lastRecycledId) {
                        throw new IllegalStateException("recycled already");
                    }
                    // 将可转移成功的DefaultHandle元素的stack属性设置为目的地Stack
                    element.stack = dst;
                    // 将DefaultHandle元素转移到目的地Stack的DefaultHandle[newDstSize ++]中
                    dstElems[newDstSize++] = element;
                    // 设置为null，清楚暂存的handler信息，同时帮助 GC
                    srcElems[i] = null;
                }
                // 将新的newDstSize赋值给目的地Stack的size
                dst.size = newDstSize;

                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    // 将Head指向下一个Link，也就是将当前的Link给回收掉了
                    // 假设之前为Head -> Link1 -> Link2，回收之后为Head -> Link2
                    this.head = head.next;
                }
                // 设置读指针位置
                head.readIndex = srcEnd;
                return true;
            } else {
                // The destination stack is full already.
                return false;
            }
        }
    }

    static final class Stack<T> {

        // we keep a queue of per-thread queues, which is appended to once only, each time a new thread other
        // than the stack owner recycles: when we run out of items in our stack we iterate this collection
        // to scavenge those that can be reused. this permits us to incur minimal thread synchronisation whilst
        // still recycling all items.
        final Recyclers<T> parent;
        final Thread thread;
        private DefaultHandle[] elements;
        private final int maxCapacity;
        private int size;

        private volatile WeakOrderQueue head;
        // cursor：当前操作的WeakOrderQueue
        //prev：cursor的前一个WeakOrderQueue
        private WeakOrderQueue cursor, prev;

        Stack(Recyclers<T> parent, Thread thread, int maxCapacity) {
            this.parent = parent;
            this.thread = thread;
            this.maxCapacity = maxCapacity;
            elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
        }

        int increaseCapacity(int expectedCapacity) {
            int newCapacity = elements.length;
            int maxCapacity = this.maxCapacity;
            do {
                newCapacity <<= 1;
            } while (newCapacity < expectedCapacity && newCapacity < maxCapacity);

            newCapacity = Math.min(newCapacity, maxCapacity);
            if (newCapacity != elements.length) {
                elements = Arrays.copyOf(elements, newCapacity);
            }

            return newCapacity;
        }

        DefaultHandle pop() {
            int size = this.size;
            // size=0 则说明本线程的Stack没有可用的对象，先从其它线程中获取。
            if (size == 0) {
                // 当 Stack<T> 此时的容量为 0 时，去 WeakOrder 中转移部分对象到 Stack 中
                if (!scavenge()) {
                    return null;
                }
                //由于在transfer(Stack<?> dst)的过程中，可能会将其他线程的WeakOrderQueue中的DefaultHandle对象传递到当前的Stack,
                //所以size发生了变化，需要重新赋值
                size = this.size;
            }
            //size表示整个stack中的大小
            size--;
            //获取最后一个元素
            DefaultHandle ret = elements[size];
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            // 清空回收信息，以便判断是否重复回收
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            this.size = size;
            return ret;
        }

        boolean scavenge() {
            // continue an existing scavenge, if any
            // 扫描判断是否存在可转移的 Handler
            if (scavengeSome()) {
                return true;
            }

            // reset our scavenge cursor
            prev = null;
            cursor = head;
            return false;
        }

        boolean scavengeSome() {
            WeakOrderQueue cursor = this.cursor;
            if (cursor == null) {
                cursor = head;
                // 如果head==null，表示当前的Stack对象没有WeakOrderQueue，直接返回
                if (cursor == null) {
                    return false;
                }
            }

            boolean success = false;
            WeakOrderQueue prev = this.prev;
            do {
                // 从当前的WeakOrderQueue节点进行 handler 的转移
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }
                // 遍历下一个WeakOrderQueue
                WeakOrderQueue next = cursor.next;
                // 如果 WeakOrderQueue 的实际持有线程因GC回收了
                if (cursor.owner.get() == null) {
                    // If the thread associated with the queue is gone, unlink it, after
                    // performing a volatile read to confirm there is no data left to collect.
                    // We never unlink the first queue, as we don't want to synchronize on updating the head.
                    // 如果当前的WeakOrderQueue的线程已经不可达了
                    //如果该WeakOrderQueue中有数据，则将其中的数据全部转移到当前Stack中
                    if (cursor.hasFinalData()) {
                        for (;;) {
                            if (cursor.transfer(this)) {
                                success = true;
                            } else {
                                break;
                            }
                        }
                    }
                    //将当前的WeakOrderQueue的前一个节点prev指向当前的WeakOrderQueue的下一个节点，
                    // 即将当前的WeakOrderQueue从Queue链表中移除。方便后续GC
                    if (prev != null) {
                        prev.next = next;
                    }
                } else {
                    prev = cursor;
                }

                cursor = next;

            } while (cursor != null && !success);

            this.prev = prev;
            this.cursor = cursor;
            return success;
        }


        void push(DefaultHandle item) {
            // (item.recycleId | item.lastRecycleId) != 0 等价于 item.recycleId!=0 && item.lastRecycleId!=0
            // 当item开始创建时item.recycleId==0 && item.lastRecycleId==0
            // 当item被recycle时，item.recycleId==x，item.lastRecycleId==y 进行赋值
            // 当item被pop之后， item.recycleId = item.lastRecycleId = 0
            // 所以当item.recycleId 和 item.lastRecycleId 任何一个不为0，则表示回收过
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            // 设置对象的回收id为线程id信息，标记自己的被回收的线程信息
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            if (size >= maxCapacity) {
                // Hit the maximum capacity - drop the possibly youngest object.
                return;
            }
            // stack中的elements扩容两倍，复制元素，将新数组赋值给stack.elements
            if (size == elements.length) {
                elements = Arrays.copyOf(elements, Math.min(size << 1, maxCapacity));
            }

            elements[size] = item;
            this.size = size + 1;
        }

        DefaultHandle newHandle() {
            return new DefaultHandle(this);
        }
    }
}
