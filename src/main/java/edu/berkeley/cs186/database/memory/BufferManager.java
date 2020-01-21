package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;

import java.util.function.BiConsumer;

public interface BufferManager extends AutoCloseable {
    // We reserve 36 bytes on each page for bookkeeping for recovery
    // (used to store the pageLSN, and to ensure that a redo-only/undo-only log record can
    // fit on one page).
    short RESERVED_SPACE = 36;

    // Effective page size available to users of buffer manager.
    short EFFECTIVE_PAGE_SIZE = DiskSpaceManager.PAGE_SIZE - RESERVED_SPACE;

    @Override
    void close();

    /**
     * Fetches the specified page, with a loaded and pinned buffer frame.
     *
     * @param parentContext lock context of the **parent** of the page being fetched
     * @param pageNum       page number
     * @param logPage       whether the page is for the log or not
     * @return specified page
     */
    Page fetchPage(LockContext parentContext, long pageNum, boolean logPage);

    /**
     * Fetches a new page, with a loaded and pinned buffer frame.
     *
     * @param parentContext parent lock context of the new page
     * @param partNum       partition number for new page
     * @param logPage       whether the page is for the log or not
     * @return the new page
     */
    Page fetchNewPage(LockContext parentContext, int partNum, boolean logPage);

    /**
     * Frees a page - evicts the page from cache, and tells the disk space manager
     * that the page is no longer needed. Page must be pinned before this call,
     * and cannot be used after this call (aside from unpinning).
     *
     * @param page page to free
     */
    void freePage(Page page);

    /**
     * Frees a partition - evicts all relevant pages from cache, and tells the disk space manager
     * that the partition is no longer needed. No pages in the partition may be pinned before this call,
     * and cannot be used after this call.
     *
     * @param partNum partition number to free
     */
    void freePart(int partNum);

    /**
     * Fetches a buffer frame with data for the specified page. Reuses existing buffer frame
     * if page already loaded in memory. Pins the buffer frame. Cannot be used outside the package.
     *
     * @param pageNum page number
     * @param logPage whether the page is for the log or not
     * @return buffer frame with specified page loaded
     */
    BufferFrame fetchPageFrame(long pageNum, boolean logPage);

    /**
     * Fetches a buffer frame for a new page. Pins the buffer frame. Cannot be used outside the package.
     *
     * @param partNum partition number for new page
     * @param logPage whether the page is for the log or not
     * @return buffer frame for the new page
     */
    BufferFrame fetchNewPageFrame(int partNum, boolean logPage);

    /**
     * Calls flush on the frame of a page and unloads the page from the frame. If the page
     * is not loaded, this does nothing.
     * @param pageNum page number of page to evict
     */
    void evict(long pageNum);

    /**
     * Calls evict on every frame in sequence.
     */
    void evictAll();

    /**
     * Calls the passed in method with the page number of every loaded page.
     * @param process method to consume page numbers. The first parameter is the page number,
     *                and the second parameter is a boolean indicating whether the page is dirty
     *                (has an unflushed change).
     */
    void iterPageNums(BiConsumer<Long, Boolean> process);

    /**
     * Get the number of I/Os since the buffer manager was started, excluding anything used in disk
     * space management, and not counting allocation/free. This is not really useful except as a
     * relative measure.
     * @return number of I/Os
     */
    long getNumIOs();
}
