package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Implementation of a buffer manager, with configurable page replacement policies.
 * Data is stored in page-sized byte arrays, and returned in a Frame object specific
 * to the page loaded (evicting and loading a new page into the frame will result in
 * a new Frame object, with the same underlying byte array), with old Frame objects
 * backed by the same byte array marked as invalid.
 */
public class BufferManagerImpl implements BufferManager {
    // Buffer frames
    private Frame[] frames;

    // Reference to the disk space manager underneath this buffer manager instance.
    private DiskSpaceManager diskSpaceManager;

    // Map of page number to frame index
    private Map<Long, Integer> pageToFrame;

    // Lock on buffer manager
    private ReentrantLock managerLock;

    // Eviction policy
    private EvictionPolicy evictionPolicy;

    // Index of first free frame
    private int firstFreeIndex;

    // Recovery manager
    private RecoveryManager recoveryManager;

    // Count of number of I/Os
    private long numIOs = 0;

    /**
     * Buffer frame, containing information about the loaded page, wrapped around the
     * underlying byte array. Free frames use the index field to create a (singly) linked
     * list between free frames.
     */
    class Frame extends BufferFrame {
        private static final int INVALID_INDEX = Integer.MIN_VALUE;

        byte[] contents;
        private int index;
        private long pageNum;
        private boolean dirty;
        private ReentrantLock frameLock;
        private boolean logPage;

        Frame(byte[] contents, int nextFree, boolean logPage) {
            this(contents, ~nextFree, DiskSpaceManager.INVALID_PAGE_NUM, logPage);
        }

        Frame(Frame frame) {
            this(frame.contents, frame.index, frame.pageNum, frame.logPage);
        }

        Frame(byte[] contents, int index, long pageNum, boolean logPage) {
            this.contents = contents;
            this.index = index;
            this.pageNum = pageNum;
            this.dirty = false;
            this.frameLock = new ReentrantLock();
            this.logPage = logPage;
        }

        /**
         * Pin buffer frame; cannot be evicted while pinned. A "hit" happens when the
         * buffer frame gets pinned.
         */
        @Override
        public void pin() {
            this.frameLock.lock();

            if (!this.isValid()) {
                throw new IllegalStateException("pinning invalidated frame");
            }

            super.pin();
        }

        /**
         * Unpin buffer frame.
         */
        @Override
        public void unpin() {
            super.unpin();
            this.frameLock.unlock();
        }

        /**
         * @return whether this frame is valid
         */
        @Override
        public boolean isValid() {
            return this.index >= 0;
        }

        /**
         * @return whether this frame's page has been freed
         */
        private boolean isFreed() {
            return this.index < 0 && this.index != INVALID_INDEX;
        }

        /**
         * Invalidates the frame, flushing it if necessary.
         */
        private void invalidate() {
            if (this.isValid()) {
                this.flush();
            }
            this.index = INVALID_INDEX;
            this.contents = null;
        }

        /**
         * Marks the frame as free.
         */
        private void setFree() {
            if (isFreed()) {
                throw new IllegalStateException("cannot free free frame");
            }
            int nextFreeIndex = firstFreeIndex;
            firstFreeIndex = this.index;
            this.index = ~nextFreeIndex;
        }

        private void setUsed() {
            if (!isFreed()) {
                throw new IllegalStateException("cannot unfree used frame");
            }
            int index = firstFreeIndex;
            firstFreeIndex = ~this.index;
            this.index = index;
        }

        /**
         * @return page number of this frame
         */
        @Override
        public long getPageNum() {
            return this.pageNum;
        }

        /**
         * Flushes this buffer frame to disk, but does not unload it.
         */
        @Override
        void flush() {
            this.frameLock.lock();
            super.pin();
            try {
                if (!this.isValid()) {
                    return;
                }
                if (!this.dirty) {
                    return;
                }
                if (!this.logPage) {
                    recoveryManager.pageFlushHook(this.getPageLSN());
                }
                BufferManagerImpl.this.diskSpaceManager.writePage(pageNum, contents);
                BufferManagerImpl.this.incrementIOs();
                this.dirty = false;
            } finally {
                super.unpin();
                this.frameLock.unlock();
            }
        }

        /**
         * Read from the buffer frame.
         * @param position position in buffer frame to start reading
         * @param num number of bytes to read
         * @param buf output buffer
         */
        @Override
        void readBytes(short position, short num, byte[] buf) {
            this.pin();
            try {
                if (!this.isValid()) {
                    throw new IllegalStateException("reading from invalid buffer frame");
                }
                System.arraycopy(this.contents, position + dataOffset(), buf, 0, num);
                BufferManagerImpl.this.evictionPolicy.hit(this);
            } finally {
                this.unpin();
            }
        }

        /**
         * Write to the buffer frame, and mark frame as dirtied.
         * @param position position in buffer frame to start writing
         * @param num number of bytes to write
         * @param buf input buffer
         */
        @Override
        void writeBytes(short position, short num, byte[] buf) {
            this.pin();
            try {
                if (!this.isValid()) {
                    throw new IllegalStateException("writing to invalid buffer frame");
                }
                int offset = position + dataOffset();
                TransactionContext transaction = TransactionContext.getTransaction();
                if (transaction != null && !logPage) {
                    List<Pair<Integer, Integer>> changedRanges = getChangedBytes(offset, num, buf);
                    for (Pair<Integer, Integer> range : changedRanges) {
                        int start = range.getFirst();
                        int len = range.getSecond();
                        byte[] before = Arrays.copyOfRange(contents, start + offset, start + offset + len);
                        byte[] after = Arrays.copyOfRange(buf, start, start + len);
                        long pageLSN = recoveryManager.logPageWrite(transaction.getTransNum(), pageNum, position, before,
                                       after);
                        this.setPageLSN(pageLSN);
                    }
                }
                System.arraycopy(buf, 0, this.contents, offset, num);
                this.dirty = true;
                BufferManagerImpl.this.evictionPolicy.hit(this);
            } finally {
                this.unpin();
            }
        }

        /**
         * Requests a valid Frame object for the page (if invalid, a new Frame object is returned).
         * Page is pinned on return.
         */
        @Override
        Frame requestValidFrame() {
            this.frameLock.lock();
            try {
                if (this.isFreed()) {
                    throw new PageException("page already freed");
                }
                if (this.isValid()) {
                    this.pin();
                    return this;
                }
                return BufferManagerImpl.this.fetchPageFrame(this.pageNum, logPage);
            } finally {
                this.frameLock.unlock();
            }
        }

        @Override
        short getEffectivePageSize() {
            if (logPage) {
                return DiskSpaceManager.PAGE_SIZE;
            } else {
                return BufferManager.EFFECTIVE_PAGE_SIZE;
            }
        }

        @Override
        long getPageLSN() {
            return ByteBuffer.wrap(this.contents).getLong(8);
        }

        @Override
        public String toString() {
            if (index >= 0) {
                return "Buffer Frame " + index + ", Page " + pageNum + (isPinned() ? " (pinned)" : "");
            } else if (index == INVALID_INDEX) {
                return "Buffer Frame (evicted), Page " + pageNum;
            } else {
                return "Buffer Frame (freed), next free = " + (~index);
            }
        }

        /**
         * Generates (offset, length) pairs for where buf differs from contents. Merges nearby
         * pairs (where nearby is defined as pairs that have fewer than BufferManager.RESERVED_SPACE
         * bytes of unmodified data between them).
         */
        private List<Pair<Integer, Integer>> getChangedBytes(int offset, int num, byte[] buf) {
            List<Pair<Integer, Integer>> ranges = new ArrayList<>();
            int startIndex = -1;
            int skip = -1;
            for (int i = 0; i < num; ++i) {
                if (buf[i] == contents[offset + i] && startIndex >= 0) {
                    if (skip > BufferManager.RESERVED_SPACE) {
                        ranges.add(new Pair<>(startIndex, i - startIndex - skip));
                        startIndex = -1;
                        skip = -1;
                    } else {
                        ++skip;
                    }
                } else if (buf[i] != contents[offset + i]) {
                    if (startIndex < 0) {
                        startIndex = i;
                    }
                    skip = 0;
                }
            }
            if (startIndex >= 0) {
                ranges.add(new Pair<>(startIndex, num - startIndex - skip));
            }
            return ranges;
        }

        void setPageLSN(long pageLSN) {
            ByteBuffer.wrap(this.contents).putLong(8, pageLSN);
        }

        private short dataOffset() {
            if (logPage) {
                return 0;
            } else {
                return BufferManager.RESERVED_SPACE;
            }
        }
    }

    /**
     * Creates a new buffer manager.
     *
     * @param diskSpaceManager the underlying disk space manager
     * @param bufferSize size of buffer (in pages)
     * @param evictionPolicy eviction policy to use
     */
    public BufferManagerImpl(DiskSpaceManager diskSpaceManager, RecoveryManager recoveryManager,
                             int bufferSize, EvictionPolicy evictionPolicy) {
        this.frames = new Frame[bufferSize];
        for (int i = 0; i < bufferSize; ++i) {
            this.frames[i] = new Frame(new byte[DiskSpaceManager.PAGE_SIZE], i + 1, false);
        }
        this.firstFreeIndex = 0;
        this.diskSpaceManager = diskSpaceManager;
        this.pageToFrame = new HashMap<>();
        this.managerLock = new ReentrantLock();
        this.evictionPolicy = evictionPolicy;
        this.recoveryManager = recoveryManager;
    }

    @Override
    public void close() {
        this.managerLock.lock();
        try {
            for (Frame frame : this.frames) {
                frame.frameLock.lock();
                try {
                    if (frame.isPinned()) {
                        throw new IllegalStateException("closing buffer manager but frame still pinned");
                    }
                    if (!frame.isValid()) {
                        continue;
                    }
                    evictionPolicy.cleanup(frame);
                    frame.invalidate();
                } finally {
                    frame.frameLock.unlock();
                }
            }
        } finally {
            this.managerLock.unlock();
        }
    }

    @Override
    public Frame fetchPageFrame(long pageNum, boolean logPage) {
        this.managerLock.lock();
        Frame newFrame;
        Frame evictedFrame;
        // figure out what frame to load data to, and update manager state
        try {
            if (!this.diskSpaceManager.pageAllocated(pageNum)) {
                throw new PageException("page " + pageNum + " not allocated");
            }
            if (this.pageToFrame.containsKey(pageNum)) {
                newFrame = this.frames[this.pageToFrame.get(pageNum)];
                newFrame.pin();
                return newFrame;
            }
            // prioritize free frames over eviction
            if (this.firstFreeIndex < this.frames.length) {
                evictedFrame = this.frames[this.firstFreeIndex];
                evictedFrame.setUsed();
            } else {
                evictedFrame = (Frame) evictionPolicy.evict(frames);
                this.pageToFrame.remove(evictedFrame.pageNum, evictedFrame.index);
                evictionPolicy.cleanup(evictedFrame);
            }
            int frameIndex = evictedFrame.index;
            newFrame = this.frames[frameIndex] = new Frame(evictedFrame.contents, frameIndex, pageNum, logPage);
            evictionPolicy.init(newFrame);

            evictedFrame.frameLock.lock();
            newFrame.frameLock.lock();

            this.pageToFrame.put(pageNum, frameIndex);
        } finally {
            this.managerLock.unlock();
        }
        // flush evicted frame
        try {
            evictedFrame.invalidate();
        } finally {
            evictedFrame.frameLock.unlock();
        }
        // read new page into frame
        try {
            newFrame.pageNum = pageNum;
            newFrame.pin();
            BufferManagerImpl.this.diskSpaceManager.readPage(pageNum, newFrame.contents);
            this.incrementIOs();
            return newFrame;
        } catch (PageException e) {
            newFrame.unpin();
            throw e;
        } finally {
            newFrame.frameLock.unlock();
        }
    }

    @Override
    public Page fetchPage(LockContext parentContext, long pageNum, boolean logPage) {
        return this.frameToPage(parentContext, pageNum, this.fetchPageFrame(pageNum, logPage));
    }

    @Override
    public Frame fetchNewPageFrame(int partNum, boolean logPage) {
        long pageNum = this.diskSpaceManager.allocPage(partNum);
        this.managerLock.lock();
        try {
            return fetchPageFrame(pageNum, logPage);
        } finally {
            this.managerLock.unlock();
        }
    }

    @Override
    public Page fetchNewPage(LockContext parentContext, int partNum, boolean logPage) {
        Frame newFrame = this.fetchNewPageFrame(partNum, logPage);
        return this.frameToPage(parentContext, newFrame.getPageNum(), newFrame);
    }

    @Override
    public void freePage(Page page) {
        this.managerLock.lock();
        try {
            int frameIndex = this.pageToFrame.get(page.getPageNum());
            Frame frame = this.frames[frameIndex];
            this.pageToFrame.remove(page.getPageNum(), frameIndex);
            evictionPolicy.cleanup(frame);
            frame.setFree();

            this.frames[frameIndex] = new Frame(frame);
            diskSpaceManager.freePage(page.getPageNum());
        } finally {
            this.managerLock.unlock();
        }
    }

    @Override
    public void freePart(int partNum) {
        this.managerLock.lock();
        try {
            for (int i = 0; i < frames.length; ++i) {
                Frame frame = frames[i];
                if (DiskSpaceManager.getPartNum(frame.pageNum) == partNum) {
                    this.pageToFrame.remove(frame.getPageNum(), i);
                    evictionPolicy.cleanup(frame);
                    frame.setFree();

                    frames[i] = new Frame(frame);
                }
            }

            diskSpaceManager.freePart(partNum);
        } finally {
            this.managerLock.unlock();
        }
    }

    @Override
    public void evict(long pageNum) {
        managerLock.lock();
        try {
            if (!pageToFrame.containsKey(pageNum)) {
                return;
            }
            evict(pageToFrame.get(pageNum));
        } finally {
            managerLock.unlock();
        }
    }

    private void evict(int i) {
        Frame frame = frames[i];
        frame.frameLock.lock();
        try {
            if (frame.isValid() && !frame.isPinned()) {
                this.pageToFrame.remove(frame.pageNum, frame.index);
                evictionPolicy.cleanup(frame);

                frames[i] = new Frame(frame.contents, this.firstFreeIndex, false);
                this.firstFreeIndex = i;

                frame.invalidate();
            }
        } finally {
            frame.frameLock.unlock();
        }
    }

    @Override
    public void evictAll() {
        for (int i = 0; i < frames.length; ++i) {
            evict(i);
        }
    }

    @Override
    public void iterPageNums(BiConsumer<Long, Boolean> process) {
        for (Frame frame : frames) {
            frame.frameLock.lock();
            try {
                if (frame.isValid()) {
                    process.accept(frame.pageNum, frame.dirty);
                }
            } finally {
                frame.frameLock.unlock();
            }
        }
    }

    @Override
    public long getNumIOs() {
        return numIOs;
    }

    private void incrementIOs() {
        ++numIOs;
    }

    /**
     * Wraps a frame in a page object.
     * @param parentContext parent lock context of the page
     * @param pageNum page number
     * @param frame frame for the page
     * @return page object
     */
    private Page frameToPage(LockContext parentContext, long pageNum, Frame frame) {
        return new Page(parentContext.childContext(pageNum), frame);
    }
}
