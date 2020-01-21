package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.common.iterator.BacktrackingIterable;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.memory.Page;

/**
 * Interface for a heap file, which receives requests for pages with
 * a certain amount of space, and returns a page with enough space.
 * Assumes a packed page layout.
 */
public interface HeapFile extends BacktrackingIterable<Page> {
    /**
     * @return effective page size (including metadata).
     */
    short getEffectivePageSize();

    /**
     * Sets the size of metadata on an empty page.
     * @param emptyPageMetadataSize amount of metadata on empty page
     */
    void setEmptyPageMetadataSize(short emptyPageMetadataSize);

    /**
     * Fetches a specific pinned page.
     * @param pageNum page number
     * @return the pinned page
     */
    Page getPage(long pageNum);

    /**
     * Fetches a data page with a certain amount of unused space. New data and
     * header pages may be allocated as necessary.
     * @param requiredSpace amount of space needed;
     *                      cannot be larger than effectivePageSize - emptyPageMetadataSize
     * @return pinned page with the requested area of contiguous space
     */
    Page getPageWithSpace(short requiredSpace);

    /**
     * Updates the amount of free space on a page. Updating to effectivePageSize
     * frees the page, and it may no longer be used.
     * @param page the data page returned by getPageWithSpace
     * @param newFreeSpace the size (in bytes) of the free space on the page
     */
    void updateFreeSpace(Page page, short newFreeSpace);

    /**
     * @return iterator of all allocated data pages
     */
    @Override
    BacktrackingIterator<Page> iterator();

    /**
     * Returns estimate of number of data pages.
     * @return estimate of number of data pages
     */
    int getNumDataPages();

    /**
     * Gets partition number of partition the heap file lies on.
     * @return partition number
     */
    int getPartNum();
}
