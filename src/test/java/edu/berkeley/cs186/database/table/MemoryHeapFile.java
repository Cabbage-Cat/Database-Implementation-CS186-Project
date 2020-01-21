package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.IndexBacktrackingIterator;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;

import java.util.*;

/**
 * Heap file implementation that is entirely in memory. Not thread safe.
 */
public class MemoryHeapFile implements HeapFile, AutoCloseable {
    private List<Long> pageNums = new ArrayList<>();
    private Map<Long, Page> pages = new HashMap<>();
    private Map<Long, Short> freeSpace = new HashMap<>();
    private short emptyPageMetadataSize = 0;
    private BufferManager bufferManager;
    private int numDataPages = 0;

    public MemoryHeapFile() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManagerImpl(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
    }

    @Override
    public void close() {
        this.bufferManager.close();
    }

    @Override
    public short getEffectivePageSize() {
        return BufferManager.EFFECTIVE_PAGE_SIZE;
    }

    @Override
    public void setEmptyPageMetadataSize(short emptyPageMetadataSize) {
        this.emptyPageMetadataSize = emptyPageMetadataSize;
    }

    @Override
    public Page getPage(long pageNum) {
        if (!pages.containsKey(pageNum) || pages.get(pageNum) == null) {
            throw new PageException("page not found");
        }
        return bufferManager.fetchPage(new DummyLockContext(), pageNum, false);
    }

    @Override
    public Page getPageWithSpace(short requiredSpace) {
        for (Map.Entry<Long, Short> entry : freeSpace.entrySet()) {
            if (entry.getValue() >= requiredSpace) {
                freeSpace.put(entry.getKey(), (short) (entry.getValue() - requiredSpace));
                return bufferManager.fetchPage(new DummyLockContext(), entry.getKey(), false);
            }
        }
        Page page = bufferManager.fetchNewPage(new DummyLockContext(), 0, false);
        pageNums.add(page.getPageNum());
        pages.put(page.getPageNum(), page);
        freeSpace.put(page.getPageNum(),
                      (short)  (getEffectivePageSize() - emptyPageMetadataSize - requiredSpace));
        ++numDataPages;
        return page;
    }

    @Override
    public void updateFreeSpace(Page page, short newFreeSpace) {
        if (newFreeSpace == getEffectivePageSize() - emptyPageMetadataSize) {
            pages.put(page.getPageNum(), null);
            --numDataPages;
        }
        freeSpace.put(page.getPageNum(), newFreeSpace);
    }

    @Override
    public BacktrackingIterator<Page> iterator() {
        return new PageIterator();
    }

    @Override
    public int getNumDataPages() {
        return numDataPages;
    }

    @Override
    public int getPartNum() {
        return 0;
    }

    private class PageIterator extends IndexBacktrackingIterator<Page> {
        PageIterator() {
            super(pageNums.size());
        }

        @Override
        protected int getNextNonempty(int currentIndex) {
            ++currentIndex;
            while (currentIndex < pageNums.size()) {
                Page page = getValue(currentIndex);
                if (page != null) {
                    page.unpin();
                    break;
                }
                ++currentIndex;
            }
            return currentIndex;
        }

        @Override
        protected Page getValue(int index) {
            return bufferManager.fetchPage(new DummyLockContext(), pageNums.get(index), false);
        }
    }
}
