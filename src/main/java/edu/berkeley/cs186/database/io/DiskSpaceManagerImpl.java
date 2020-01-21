package edu.berkeley.cs186.database.io;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Bits;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of a disk space manager with virtual page translation, and
 * two levels of header pages, allowing for (with page size of 4K) 256G worth of data per partition:
 *
 *                                           [master page]
 *                  /                              |                               \
 *           [header page]                                                    [header page]
 *     /    |     |     |     \                   ...                   /    |     |     |     \
 * [data] [data] ... [data] [data]                                   [data] [data] ... [data] [data]
 *
 * Each header page stores a bitmap, indicating whether each of the data pages has been allocated,
 * and manages 32K pages. The master page stores 16-bit integers for each of the header pages indicating
 * the number of data pages that have been allocated under the header page (managing 2K header pages).
 * A single partition may therefore have a maximum of 64M data pages.
 *
 * Master and header pages are cached permanently in memory; changes to these are immediately flushed to
 * disk. This imposes a fairly small memory overhead (128M partitions have 2 pages cached). This caching
 * is done separately from the buffer manager's caching.
 *
 * Virtual page numbers are 64-bit integers (Java longs) assigned to data pages in the following format:
 *       partition number * 10^10 + n
 * for the n-th data page of the partition (indexed from 0). This particular format (instead of a simpler
 * scheme such as assigning the upper 32 bits to partition number and lower 32 to page number) was chosen
 * for ease of debugging (it's easier to read 10000000006 as part 1 page 6, than it is to decipher 4294967302).
 *
 * Partitions are backed by OS level files (one OS level file per partition), and are stored in the following
 * manner:
 * - the master page is the 0th page of the OS file
 * - the first header page is the 1st page of the OS file
 * - the next 32K pages are data pages managed by the first header page
 * - the second header page follows
 * - the next 32K pages are data pages managed by the second header page
 * - etc.
 */
public class DiskSpaceManagerImpl implements DiskSpaceManager {
    private static final int MAX_HEADER_PAGES = PAGE_SIZE / 2; // 2 bytes per header page
    private static final int DATA_PAGES_PER_HEADER = PAGE_SIZE * 8; // 1 bit per data page

    // Name of base directory.
    private String dbDir;

    // Info about each partition.
    private Map<Integer, PartInfo> partInfo;

    // Counter to generate new partition numbers.
    private AtomicInteger partNumCounter;

    // Lock on the entire manager.
    private ReentrantLock managerLock;

    // recovery manager
    private RecoveryManager recoveryManager;

    private static class PartInfo implements AutoCloseable {
        // Underyling OS file/file channel.
        private RandomAccessFile file;
        private FileChannel fileChannel;

        // Lock on the partition.
        private ReentrantLock partitionLock;

        // Contents of the master page of this partition
        private int[] masterPage;

        // Contents of the various header pages of this partition
        private List<byte[]> headerPages;

        // Recovery manager
        private RecoveryManager recoveryManager;

        // Partition number
        private int partNum;

        private PartInfo(int partNum, RecoveryManager recoveryManager) {
            this.masterPage = new int[MAX_HEADER_PAGES];
            this.headerPages = new ArrayList<>();
            this.partitionLock = new ReentrantLock();
            this.recoveryManager = recoveryManager;
            this.partNum = partNum;
        }

        /**
         * Opens the OS file and loads master and header pages.
         * @param fileName name of OS file partition is stored in
         */
        private void open(String fileName) {
            assert (this.fileChannel == null);
            try {
                this.file = new RandomAccessFile(fileName, "rw");
                this.fileChannel = this.file.getChannel();
                long length = this.file.length();
                if (length == 0) {
                    // new file, write empty master page and fill headerPages with null
                    for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
                        this.headerPages.add(null);
                    }
                    this.writeMasterPage();
                } else {
                    // old file, read in master page + header pages
                    ByteBuffer b = ByteBuffer.wrap(new byte[PAGE_SIZE]);
                    this.fileChannel.read(b, PartInfo.masterPageOffset());
                    b.position(0);
                    for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
                        this.masterPage[i] = (b.getShort() & 0xFFFF);
                        if (PartInfo.headerPageOffset(i) >= length) {
                            this.headerPages.add(null);
                        } else {
                            byte[] headerPage = new byte[PAGE_SIZE];
                            this.headerPages.add(headerPage);
                            this.fileChannel.read(ByteBuffer.wrap(headerPage), PartInfo.headerPageOffset(i));
                        }
                    }
                }
            } catch (IOException e) {
                throw new PageException("Could not open or read file: " + e.getMessage());
            }
        }

        @Override
        public void close() throws IOException {
            this.partitionLock.lock();
            try {
                this.headerPages.clear();
                this.file.close();
                this.fileChannel.close();
            } finally {
                this.partitionLock.unlock();
            }
        }

        /**
         * Writes the master page to disk.
         */
        private void writeMasterPage() throws IOException {
            ByteBuffer b = ByteBuffer.wrap(new byte[PAGE_SIZE]);
            for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
                b.putShort((short) (masterPage[i] & 0xFFFF));
            }
            b.position(0);
            this.fileChannel.write(b, PartInfo.masterPageOffset());
        }

        /**
         * Writes a header page to disk.
         * @param headerIndex which header page
         */
        private void writeHeaderPage(int headerIndex) throws IOException {
            ByteBuffer b = ByteBuffer.wrap(this.headerPages.get(headerIndex));
            this.fileChannel.write(b, PartInfo.headerPageOffset(headerIndex));
        }

        /**
         * Allocates a new page in the partition.
         * @return data page number
         */
        private int allocPage() throws IOException {
            int headerIndex = -1;
            for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
                if (this.masterPage[i] < DATA_PAGES_PER_HEADER) {
                    headerIndex = i;
                    break;
                }
            }
            if (headerIndex == -1) {
                throw new PageException("no free pages - partition has reached max size");
            }

            byte[] headerBytes = this.headerPages.get(headerIndex);

            int pageIndex = -1;
            if (headerBytes == null) {
                pageIndex = 0;
            } else {
                for (int i = 0; i < DATA_PAGES_PER_HEADER; i++) {
                    if (Bits.getBit(headerBytes, i) == Bits.Bit.ZERO) {
                        pageIndex = i;
                        break;
                    }
                }
                if (pageIndex == -1) {
                    throw new PageException("header page should have free space, but doesn't");
                }
            }

            return this.allocPage(headerIndex, pageIndex);
        }

        /**
         * Allocates a new page in the partition.
         * @param headerIndex index of header page managing new page
         * @param pageIndex index within header page of new page
         * @return data page number
         */
        private int allocPage(int headerIndex, int pageIndex) throws IOException {
            byte[] headerBytes = this.headerPages.get(headerIndex);
            if (headerBytes == null) {
                headerBytes = new byte[PAGE_SIZE];
                this.headerPages.remove(headerIndex);
                this.headerPages.add(headerIndex, headerBytes);
            }

            if (Bits.getBit(headerBytes, pageIndex) == Bits.Bit.ONE) {
                throw new IllegalStateException("page at (part=" + partNum + ", header=" + headerIndex + ", index="
                                                +
                                                pageIndex + ") already allocated");
            }

            Bits.setBit(headerBytes, pageIndex, Bits.Bit.ONE);
            this.masterPage[headerIndex] = Bits.countBits(headerBytes);

            int pageNum = pageIndex + headerIndex * DATA_PAGES_PER_HEADER;

            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                long vpn = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
                recoveryManager.logAllocPage(transaction.getTransNum(), vpn);
                recoveryManager.diskIOHook(vpn);
            }

            this.writeMasterPage();
            this.writeHeaderPage(headerIndex);

            return pageNum;
        }

        /**
         * Frees a page in the partition from use.
         * @param pageNum data page number to be freed
         */
        private void freePage(int pageNum) throws IOException {
            int headerIndex = pageNum / DATA_PAGES_PER_HEADER;
            int pageIndex = pageNum % DATA_PAGES_PER_HEADER;

            byte[] headerBytes = headerPages.get(headerIndex);
            if (headerBytes == null) {
                throw new NoSuchElementException("cannot free unallocated page");
            }

            if (Bits.getBit(headerBytes, pageIndex) == Bits.Bit.ZERO) {
                throw new NoSuchElementException("cannot free unallocated page");
            }

            Bits.setBit(headerBytes, pageIndex, Bits.Bit.ONE);
            this.masterPage[headerIndex] = Bits.countBits(headerBytes);

            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                long vpn = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
                recoveryManager.logFreePage(transaction.getTransNum(), vpn);
                recoveryManager.diskIOHook(vpn);
            }

            this.writeMasterPage();
            this.writeHeaderPage(headerIndex);
        }

        /**
         * Reads in a data page. Assumes that the partition lock is held.
         * @param pageNum data page number to read in
         * @param buf output buffer to be filled with page - assumed to be page size
         */
        private void readPage(int pageNum, byte[] buf) throws IOException {
            if (this.isNotAllocatedPage(pageNum)) {
                throw new PageException("page " + pageNum + " is not allocated");
            }
            ByteBuffer b = ByteBuffer.wrap(buf);
            this.fileChannel.read(b, PartInfo.dataPageOffset(pageNum));
        }

        /**
         * Writes to a data page. Assumes that the partition lock is held.
         * @param pageNum data page number to write to
         * @param buf input buffer with new contents of page - assumed to be page size
         */
        private void writePage(int pageNum, byte[] buf) throws IOException {
            if (this.isNotAllocatedPage(pageNum)) {
                throw new PageException("page " + pageNum + " is not allocated");
            }
            ByteBuffer b = ByteBuffer.wrap(buf);
            this.fileChannel.write(b, PartInfo.dataPageOffset(pageNum));
            this.fileChannel.force(false);

            long vpn = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
            recoveryManager.diskIOHook(vpn);
        }

        /**
         * Checks if page number is for an unallocated data page
         * @param pageNum data page number
         * @return true if page is not valid or not allocated
         */
        private boolean isNotAllocatedPage(int pageNum) {
            int headerIndex = pageNum / DATA_PAGES_PER_HEADER;
            int pageIndex = pageNum % DATA_PAGES_PER_HEADER;
            if (headerIndex < 0 || headerIndex >= MAX_HEADER_PAGES) {
                return true;
            }
            if (masterPage[headerIndex] == 0) {
                return true;
            }
            return Bits.getBit(headerPages.get(headerIndex), pageIndex) == Bits.Bit.ZERO;
        }

        /**
         * @return offset in OS file for master page
         */
        private static long masterPageOffset() {
            return 0;
        }

        /**
         * @param headerIndex which header page
         * @return offset in OS file for header page
         */
        private static long headerPageOffset(int headerIndex) {
            return (long) (1 + headerIndex * DATA_PAGES_PER_HEADER) * PAGE_SIZE;
        }

        /**
         * @param pageNum data page number
         * @return offset in OS file for data page
         */
        private static long dataPageOffset(int pageNum) {
            return (long) (2 + pageNum / DATA_PAGES_PER_HEADER + pageNum) * PAGE_SIZE;
        }

        private void freeDataPages() throws IOException {
            for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
                if (masterPage[i] > 0) {
                    byte[] headerPage = headerPages.get(i);
                    for (int j = 0; j < DATA_PAGES_PER_HEADER; ++j) {
                        if (Bits.getBit(headerPage, j) == Bits.Bit.ONE) {
                            this.freePage(i * DATA_PAGES_PER_HEADER + j);
                        }
                    }
                }
            }
        }
    }

    /**
     * Initialize the disk space manager using the given directory. Creates the directory
     * if not present.
     *
     * @param dbDir base directory of the database
     */
    public DiskSpaceManagerImpl(String dbDir, RecoveryManager recoveryManager) {
        this.dbDir = dbDir;
        this.recoveryManager = recoveryManager;
        this.partInfo = new HashMap<>();
        this.partNumCounter = new AtomicInteger(0);
        this.managerLock = new ReentrantLock();

        File dir = new File(dbDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new PageException("could not initialize disk space manager - could not make directory");
            }
        } else {
            int maxFileNum = -1;
            File[] files = dir.listFiles();
            if (files == null) {
                throw new PageException("could not initialize disk space manager - directory is a file");
            }
            for (File f : files) {
                if (f.length() == 0) {
                    if (!f.delete()) {
                        throw new PageException("could not clean up unused file - " + f.getName());
                    }
                    continue;
                }
                int fileNum = Integer.parseInt(f.getName());
                maxFileNum = Math.max(maxFileNum, fileNum);

                PartInfo pi = new PartInfo(fileNum, recoveryManager);
                pi.open(dbDir + "/" + f.getName());
                this.partInfo.put(fileNum, pi);
            }
            this.partNumCounter.set(maxFileNum + 1);
        }
    }

    @Override
    public void close() {
        for (Map.Entry<Integer, PartInfo> part : this.partInfo.entrySet()) {
            try {
                part.getValue().close();
            } catch (IOException e) {
                throw new PageException("could not close partition " + part.getKey() + ": " + e.getMessage());
            }
        }
    }

    @Override
    public int allocPart() {
        return this.allocPartHelper(this.partNumCounter.getAndIncrement());
    }

    @Override
    public int allocPart(int partNum) {
        this.partNumCounter.updateAndGet((int x) -> Math.max(x, partNum) + 1);
        return this.allocPartHelper(partNum);
    }

    private int allocPartHelper(int partNum) {
        PartInfo pi;

        this.managerLock.lock();
        try {
            if (this.partInfo.containsKey(partNum)) {
                throw new IllegalStateException("partition number " + partNum + " already exists");
            }

            pi = new PartInfo(partNum, recoveryManager);
            this.partInfo.put(partNum, pi);

            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            // We must open partition only after logging, but we need to release the
            // manager lock first, in case the log manager is currently in the process
            // of allocating a new log page (for another txn's records).
            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                recoveryManager.logAllocPart(transaction.getTransNum(), partNum);
            }

            pi.open(dbDir + "/" + partNum);
            return partNum;
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void freePart(int partNum) {
        PartInfo pi;

        this.managerLock.lock();
        try {
            pi = this.partInfo.remove(partNum);
            if (pi == null) {
                throw new NoSuchElementException("no partition " + partNum);
            }
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            try {
                pi.freeDataPages();
                pi.close();
            } catch (IOException e) {
                throw new PageException("could not close partition " + partNum + ": " + e.getMessage());
            }

            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                recoveryManager.logFreePart(transaction.getTransNum(), partNum);
            }

            File pf = new File(dbDir + "/" + partNum);
            if (!pf.delete()) {
                throw new PageException("could not delete files for partition " + partNum);
            }
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public long allocPage(int partNum) {
        this.managerLock.lock();
        PartInfo pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            int pageNum = pi.allocPage();
            pi.writePage(pageNum, new byte[PAGE_SIZE]);
            return DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public long allocPage(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        int headerIndex = pageNum / DATA_PAGES_PER_HEADER;
        int pageIndex = pageNum % DATA_PAGES_PER_HEADER;

        this.managerLock.lock();
        PartInfo pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.allocPage(headerIndex, pageIndex);
            pi.writePage(pageNum, new byte[PAGE_SIZE]);
            return DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void freePage(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartInfo pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.freePage(pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void readPage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("readPage expects a page-sized buffer");
        }
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartInfo pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.readPage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not read partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void writePage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("writePage expects a page-sized buffer");
        }
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartInfo pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.writePage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not write partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public boolean pageAllocated(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartInfo pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            return !pi.isNotAllocatedPage(pageNum);
        } finally {
            pi.partitionLock.unlock();
        }
    }

    // Gets PartInfo, throws exception if not found.
    private PartInfo getPartInfo(int partNum) {
        PartInfo pi = this.partInfo.get(partNum);
        if (pi == null) {
            throw new NoSuchElementException("no partition " + partNum);
        }
        return pi;
    }
}
