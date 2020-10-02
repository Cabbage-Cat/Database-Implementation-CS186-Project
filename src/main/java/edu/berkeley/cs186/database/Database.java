package edu.berkeley.cs186.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.UnaryOperator;

import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.*;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.index.BPlusTree;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.io.*;
import edu.berkeley.cs186.database.memory.*;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.recovery.*;
import edu.berkeley.cs186.database.table.*;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

@SuppressWarnings("unused")
public class Database implements AutoCloseable {
    private static final String METADATA_TABLE_PREFIX = "information_schema.";
    private static final String TABLE_INFO_TABLE_NAME = METADATA_TABLE_PREFIX + "tables";
    private static final String INDEX_INFO_TABLE_NAME = METADATA_TABLE_PREFIX + "indices";
    private static final int DEFAULT_BUFFER_SIZE = 262144; // default of 1G
    private static final int MAX_SCHEMA_SIZE = 4005; // a wonderful number pulled out of nowhere

    // information_schema.tables, manages all tables in the database
    private Table tableInfo;
    // information_schema.indices, manages all indices in the database
    private Table indexInfo;
    // table name to table object mapping
    private final ConcurrentMap<String, Table> tableLookup;
    // index name to bplustree object mapping (index name is: "table,col")
    private final ConcurrentMap<String, BPlusTree> indexLookup;
    // table name to record id of entry in tableInfo
    private final ConcurrentMap<String, RecordId> tableInfoLookup;
    // index name to record id of entry in indexInfo
    private final ConcurrentMap<String, RecordId> indexInfoLookup;
    // list of indices for each table
    private final ConcurrentMap<String, List<String>> tableIndices;

    // number of transactions created
    private long numTransactions;

    // lock manager
    private final LockManager lockManager;
    // disk space manager
    private final DiskSpaceManager diskSpaceManager;
    // buffer manager
    private final BufferManager bufferManager;
    // recovery manager
    private final RecoveryManager recoveryManager;

    // transaction for creating metadata partitions and loading tables
    private final Transaction primaryInitTransaction;
    // transaction for loading indices
    private final Transaction secondaryInitTransaction;
    // thread pool for background tasks
    private final ExecutorService executor;

    // number of pages of memory to use for joins, etc.
    private int workMem = 1024; // default of 4M
    // number of pages of memory available total
    private int numMemoryPages;

    // progress in loading tables/indices
    private final Phaser loadingProgress = new Phaser(1);
    // active transactions
    private Phaser activeTransactions = new Phaser(0);

    /**
     * Creates a new database with locking disabled.
     *
     * @param fileDir the directory to put the table files in
     */
    public Database(String fileDir) {
        this (fileDir, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new database with locking disabled.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     */
    public Database(String fileDir, int numMemoryPages) {
        this(fileDir, numMemoryPages, new DummyLockManager());
        waitSetupFinished();
    }

    /**
     * Creates a new database.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager) {
        this(fileDir, numMemoryPages, lockManager, new ClockEvictionPolicy());
    }

    /**
     * Creates a new database.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     * @param policy eviction policy for buffer cache
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager,
                    EvictionPolicy policy) {
        this(fileDir, numMemoryPages, lockManager, policy, false);
    }

    /**
     * Creates a new database.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     * @param policy eviction policy for buffer cache
     * @param useRecoveryManager flag to enable or disable the recovery manager (ARIES)
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager,
                    EvictionPolicy policy, boolean useRecoveryManager) {
        boolean initialized = setupDirectory(fileDir);

        numTransactions = 0;
        this.numMemoryPages = numMemoryPages;
        this.lockManager = lockManager;
        tableLookup = new ConcurrentHashMap<>();
        indexLookup = new ConcurrentHashMap<>();
        tableIndices = new ConcurrentHashMap<>();
        tableInfoLookup = new ConcurrentHashMap<>();
        indexInfoLookup = new ConcurrentHashMap<>();
        this.executor = new ThreadPool();

        if (useRecoveryManager) {
            recoveryManager = new ARIESRecoveryManager(lockManager.databaseContext(),
                    this::beginRecoveryTranscation, this::setTransactionCounter, this::getTransactionCounter);
        } else {
            recoveryManager = new DummyRecoveryManager();
        }

        diskSpaceManager = new DiskSpaceManagerImpl(fileDir, recoveryManager);
        bufferManager = new BufferManagerImpl(diskSpaceManager, recoveryManager, numMemoryPages,
                                              policy);

        if (!initialized) {
            diskSpaceManager.allocPart(0);
        }

        recoveryManager.setManagers(diskSpaceManager, bufferManager);

        if (!initialized) {
            recoveryManager.initialize();
        }

        Runnable r = recoveryManager.restart();
        executor.submit(r);

        primaryInitTransaction = beginTransaction();
        secondaryInitTransaction = beginTransaction();
        TransactionContext.setTransaction(primaryInitTransaction.getTransactionContext());

        if (!initialized) {
            // create log partition, information_schema.tables partition, and information_schema.indices partition
            diskSpaceManager.allocPart(1);
            diskSpaceManager.allocPart(2);
        }

        TransactionContext.unsetTransaction();
        LockContext dbContext = lockManager.databaseContext();
        LockContext tableInfoContext = getTableInfoContext();

        if (!initialized) {
            dbContext.acquire(primaryInitTransaction.getTransactionContext(), LockType.X);
            this.initTableInfo();
            this.initIndexInfo();
            this.loadingProgress.arriveAndDeregister();
        } else {
            this.loadMetadataTables();
            this.loadTablesAndIndices();
        }
    }

    private boolean setupDirectory(String fileDir) {
        File dir = new File(fileDir);
        boolean initialized = dir.exists();
        if (!initialized) {
            if (!dir.mkdir()) {
                throw new DatabaseException("failed to create directory " + fileDir);
            }
        } else if (!dir.isDirectory()) {
            throw new DatabaseException(fileDir + " is not a directory");
        }
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir.toPath())) {
            initialized = initialized && dirStream.iterator().hasNext();
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return initialized;
    }

    // create information_schema.tables
    private void initTableInfo() {
        TransactionContext.setTransaction(primaryInitTransaction.getTransactionContext());

        long tableInfoPage0 = DiskSpaceManager.getVirtualPageNum(1, 0);
        diskSpaceManager.allocPage(tableInfoPage0);

        LockContext tableInfoContext = getTableInfoContext();
        HeapFile tableInfoHeapFile = new PageDirectory(bufferManager, 1, tableInfoPage0, (short) 0,
                tableInfoContext);
        tableInfo = new Table(TABLE_INFO_TABLE_NAME, getTableInfoSchema(), tableInfoHeapFile,
                              tableInfoContext);
        tableInfo.disableAutoEscalate();
        tableInfoLookup.put(TABLE_INFO_TABLE_NAME, tableInfo.addRecord(Arrays.asList(
                                new StringDataBox(TABLE_INFO_TABLE_NAME, 32),
                                new IntDataBox(1),
                                new LongDataBox(tableInfoPage0),
                                new BoolDataBox(false),
                                new StringDataBox(new String(getTableInfoSchema().toBytes()), MAX_SCHEMA_SIZE)
                            )));
        tableLookup.put(TABLE_INFO_TABLE_NAME, tableInfo);
        tableIndices.put(TABLE_INFO_TABLE_NAME, Collections.emptyList());

        primaryInitTransaction.commit();
        TransactionContext.unsetTransaction();
    }

    // create information_schema.indices
    private void initIndexInfo() {
        TransactionContext.setTransaction(secondaryInitTransaction.getTransactionContext());

        long indexInfoPage0 = DiskSpaceManager.getVirtualPageNum(2, 0);
        diskSpaceManager.allocPage(indexInfoPage0);

        LockContext indexInfoContext = getIndexInfoContext();
        HeapFile heapFile = new PageDirectory(bufferManager, 2, indexInfoPage0, (short) 0,
                                              indexInfoContext);
        indexInfo = new Table(INDEX_INFO_TABLE_NAME, getIndexInfoSchema(), heapFile, indexInfoContext);
        indexInfo.disableAutoEscalate();
        indexInfo.setFullPageRecords();
        tableInfoLookup.put(INDEX_INFO_TABLE_NAME, tableInfo.addRecord(Arrays.asList(
                                new StringDataBox(INDEX_INFO_TABLE_NAME, 32),
                                new IntDataBox(2),
                                new LongDataBox(indexInfoPage0),
                                new BoolDataBox(false),
                                new StringDataBox(new String(getIndexInfoSchema().toBytes()), MAX_SCHEMA_SIZE)
                            )));
        tableLookup.put(INDEX_INFO_TABLE_NAME, indexInfo);
        tableIndices.put(INDEX_INFO_TABLE_NAME, Collections.emptyList());

        secondaryInitTransaction.commit();
        TransactionContext.unsetTransaction();
    }

    private void loadMetadataTables() {
        // load information_schema.tables
        LockContext tableInfoContext = getTableInfoContext();
        HeapFile tableInfoHeapFile = new PageDirectory(bufferManager, 1,
                DiskSpaceManager.getVirtualPageNum(1, 0), (short) 0, tableInfoContext);
        tableInfo = new Table(TABLE_INFO_TABLE_NAME, getTableInfoSchema(), tableInfoHeapFile,
                              tableInfoContext);
        tableInfo.disableAutoEscalate();
        tableLookup.put(TABLE_INFO_TABLE_NAME, tableInfo);
        tableIndices.put(TABLE_INFO_TABLE_NAME, Collections.emptyList());
        // load information_schema.indices
        LockContext indexInfoContext = getIndexInfoContext();
        HeapFile indexInfoHeapFile = new PageDirectory(bufferManager, 2,
                DiskSpaceManager.getVirtualPageNum(2, 0), (short) 0, indexInfoContext);
        indexInfo = new Table(INDEX_INFO_TABLE_NAME, getIndexInfoSchema(), indexInfoHeapFile,
                              indexInfoContext);
        indexInfo.disableAutoEscalate();
        indexInfo.setFullPageRecords();
        tableLookup.put(INDEX_INFO_TABLE_NAME, indexInfo);
        tableIndices.put(INDEX_INFO_TABLE_NAME, Collections.emptyList());
    }

    // load tables from information_schema.tables
    private void loadTablesAndIndices() {
        Iterator<RecordId> iter = tableInfo.ridIterator();

        LockContext dbContext = lockManager.databaseContext();
        LockContext tableInfoContext = getTableInfoContext();
        TransactionContext primaryTC = primaryInitTransaction.getTransactionContext();

        dbContext.acquire(primaryTC, LockType.IX);
        tableInfoContext.acquire(primaryTC, LockType.IX);

        for (RecordId recordId : (Iterable<RecordId>) () -> iter) {
            TransactionContext.setTransaction(primaryTC);

            try {
                LockContext tableMetadataContext = tableInfoContext.childContext(recordId.getPageNum());
                // need an X lock here even though we're only reading, to prevent others from attempting to
                // fetch table object before it has been constructed
                tableMetadataContext.acquire(primaryTC, LockType.X);

                TableInfoRecord record = new TableInfoRecord(tableInfo.getRecord(recordId));
                if (!record.isAllocated()) {
                    tableInfo.deleteRecord(recordId);
                    continue;
                }

                if (record.isTemporary) {
                    continue; // no need to load temp tables - they will be cleaned up eventually by recovery
                }

                tableInfoLookup.put(record.tableName, recordId);
                tableIndices.putIfAbsent(record.tableName, Collections.synchronizedList(new ArrayList<>()));

                if (record.tableName.startsWith(METADATA_TABLE_PREFIX)) {
                    tableMetadataContext.release(primaryTC);
                    continue;
                }

                loadingProgress.register();
                executor.execute(() -> {
                    loadingProgress.arriveAndAwaitAdvance();
                    TransactionContext.setTransaction(primaryInitTransaction.getTransactionContext());

                    // X(table) acquired during table ctor; not needed earlier because no one can even check
                    // if table exists due to X(table metadata) lock
                    LockContext tableContext = getTableContext(record.tableName, record.partNum);
                    HeapFile heapFile = new PageDirectory(bufferManager, record.partNum, record.pageNum, (short) 0,
                                                          tableContext);
                    Table table = new Table(record.tableName, record.schema, heapFile, tableContext);
                    tableLookup.put(record.tableName, table);

                    // sync on lock manager to ensure that multiple jobs don't
                    // try to perform LockContext operations for the same transaction simultaneously
                    synchronized (lockManager) {
                        LockContext metadataContext = getTableInfoContext().childContext(recordId.getPageNum());

                        tableContext.release(primaryTC);
                        metadataContext.release(primaryTC);
                    }

                    TransactionContext.unsetTransaction();
                    loadingProgress.arriveAndDeregister();
                });
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        this.loadIndices();

        loadingProgress.arriveAndAwaitAdvance(); // start table/index loading

        executor.execute(() -> {
            loadingProgress.arriveAndAwaitAdvance(); // wait for all tables and indices to load
            primaryInitTransaction.commit();
            secondaryInitTransaction.commit();
            loadingProgress.arriveAndDeregister();
            // add toggleable auto-escalate
        });
    }

    // load indices from information_schema.indices
    private void loadIndices() {
        Iterator<RecordId> iter = indexInfo.ridIterator();

        LockContext dbContext = lockManager.databaseContext();
        LockContext tableInfoContext = getTableInfoContext();
        LockContext indexInfoContext = getIndexInfoContext();
        TransactionContext secondaryTC = secondaryInitTransaction.getTransactionContext();

        dbContext.acquire(secondaryTC, LockType.IX);
        tableInfoContext.acquire(secondaryTC, LockType.IS);
        indexInfoContext.acquire(secondaryTC, LockType.IX);

        for (RecordId recordId : (Iterable<RecordId>) () -> iter) {
            LockContext indexMetadataContext = indexInfoContext.childContext(recordId.getPageNum());
            // need an X lock here even though we're only reading, to prevent others from attempting to
            // fetch index object before it has been constructed
            indexMetadataContext.acquire(secondaryTC, LockType.X);

            BPlusTreeMetadata metadata = parseIndexMetadata(indexInfo.getRecord(recordId));
            if (metadata == null) {
                indexInfo.deleteRecord(recordId);
                return;
            }

            loadingProgress.register();
            executor.execute(() -> {
                RecordId tableMetadataRid = tableInfoLookup.get(metadata.getTableName());
                LockContext tableMetadataContext = tableInfoContext.childContext(tableMetadataRid.getPageNum());
                tableMetadataContext.acquire(secondaryTC, LockType.S); // S(metadata)
                LockContext tableContext = getTableContext(metadata.getTableName());
                tableContext.acquire(secondaryTC, LockType.S); // S(table)

                loadingProgress.arriveAndAwaitAdvance();

                String indexName = metadata.getName();
                LockContext indexContext = getIndexContext(indexName, metadata.getPartNum());

                try {
                    BPlusTree tree = new BPlusTree(bufferManager, metadata, indexContext);
                    if (!tableIndices.containsKey(metadata.getTableName())) {
                        // the list only needs to be synchronized while indices are being loaded, as multiple
                        // indices may attempt to add themselves to the list at the same time
                        tableIndices.put(metadata.getTableName(), Collections.synchronizedList(new ArrayList<>()));
                    }
                    tableIndices.get(metadata.getTableName()).add(indexName);
                    indexLookup.put(indexName, tree);
                    indexInfoLookup.put(indexName, recordId);

                    synchronized (loadingProgress) {
                        indexContext.release(secondaryInitTransaction.getTransactionContext());
                    }
                } finally {
                    loadingProgress.arriveAndDeregister();
                }
            });
        }
        loadingProgress.arriveAndAwaitAdvance(); // start index loading
    }

    // wait until setup has finished
    public void waitSetupFinished() {
        while (!loadingProgress.isTerminated()) {
            loadingProgress.awaitAdvance(loadingProgress.getPhase());
        }
    }

    // wait for all transactions to finish
    public synchronized void waitAllTransactions() {
        while (!activeTransactions.isTerminated()) {
            activeTransactions.awaitAdvance(activeTransactions.getPhase());
        }
    }

    /**
     * Close this database.
     */
    @Override
    public synchronized void close() {
        if (this.executor.isShutdown()) {
            return;
        }

        // wait for all transactions to terminate
        this.waitAllTransactions();

        // finish executor tasks
        this.executor.shutdown();

        this.bufferManager.evictAll();

        this.recoveryManager.close();

        this.tableInfo = null;
        this.indexInfo = null;

        this.tableLookup.clear();
        this.indexLookup.clear();
        this.tableInfoLookup.clear();
        this.indexInfoLookup.clear();
        this.tableIndices.clear();

        this.bufferManager.close();
        this.diskSpaceManager.close();
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public DiskSpaceManager getDiskSpaceManager() {
        return diskSpaceManager;
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

    @Deprecated
    public Table getTable(String tableName) {
        return tableLookup.get(prefixUserTableName(tableName));
    }

    public int getWorkMem() {
        // cap work memory at number of memory pages -- this is likely to cause out of memory
        // errors if actually set this high
        return this.workMem > this.numMemoryPages ? this.numMemoryPages : this.workMem;
    }

    public void setWorkMem(int workMem) {
        this.workMem = workMem;
    }

    // schema for information_schema.tables
    private Schema getTableInfoSchema() {
        return new Schema(
                   Arrays.asList("table_name", "part_num", "page_num", "is_temporary", "schema"),
                   Arrays.asList(Type.stringType(32), Type.intType(), Type.longType(), Type.boolType(),
                                 Type.stringType(MAX_SCHEMA_SIZE))
               );
    }

    // schema for information_schema.indices
    private Schema getIndexInfoSchema() {
        return new Schema(
                   Arrays.asList("table_name", "col_name", "order", "part_num", "root_page_num", "key_schema_typeid",
                                 "key_schema_typesize", "height"),
                   Arrays.asList(Type.stringType(32), Type.stringType(32), Type.intType(), Type.intType(),
                                 Type.longType(), Type.intType(), Type.intType(), Type.intType())
               );
    }

    // a single row of information_schema.tables
    private static class TableInfoRecord {
        String tableName;
        int partNum;
        long pageNum;
        boolean isTemporary;
        Schema schema;

        TableInfoRecord(String tableName) {
            this.tableName = tableName;
            this.partNum = -1;
            this.pageNum = -1;
            this.isTemporary = false;
            this.schema = new Schema(Collections.emptyList(), Collections.emptyList());
        }

        TableInfoRecord(Record record) {
            List<DataBox> values = record.getValues();
            tableName = values.get(0).getString();
            partNum = values.get(1).getInt();
            pageNum = values.get(2).getLong();
            isTemporary = values.get(3).getBool();
            schema = Schema.fromBytes(ByteBuffer.wrap(values.get(4).toBytes()));
        }

        List<DataBox> toDataBox() {
            return Arrays.asList(
                       new StringDataBox(tableName, 32),
                       new IntDataBox(partNum),
                       new LongDataBox(pageNum),
                       new BoolDataBox(isTemporary),
                       new StringDataBox(new String(schema.toBytes()), MAX_SCHEMA_SIZE)
                   );
        }

        boolean isAllocated() {
            return this.partNum >= 0;
        }
    }

    // row of information_schema.indices --> BPlusTreeMetadata
    private BPlusTreeMetadata parseIndexMetadata(Record record) {
        List<DataBox> values = record.getValues();
        String tableName = values.get(0).getString();
        String colName = values.get(1).getString();
        int order = values.get(2).getInt();
        int partNum = values.get(3).getInt();
        long rootPageNum = values.get(4).getLong();
        int height = values.get(7).getInt();

        if (partNum < 0) {
            return null;
        }

        Type keySchema = new Type(TypeId.values()[values.get(5).getInt()], values.get(6).getInt());
        return new BPlusTreeMetadata(tableName, colName, keySchema, order, partNum, rootPageNum, height);
    }

    // get the lock context for information_schema.tables
    private LockContext getTableInfoContext() {
        return lockManager.databaseContext().childContext(TABLE_INFO_TABLE_NAME, 1L);
    }

    // get the lock context for information_schema.indices
    private LockContext getIndexInfoContext() {
        return lockManager.databaseContext().childContext(INDEX_INFO_TABLE_NAME, 2L);
    }

    // get the lock context for a table
    private LockContext getTableContext(String table, int partNum) {
        return lockManager.databaseContext().childContext(prefixUserTableName(table), partNum);
    }

    // get the lock context for a table
    private LockContext getTableContext(String table) {
        return getTableContext(table, tableLookup.get(prefixUserTableName(table)).getPartNum());
    }

    // get the lock context for an index
    private LockContext getIndexContext(String index, int partNum) {
        return lockManager.databaseContext().childContext("indices." + index, partNum);
    }

    // get the lock context for an index
    LockContext getIndexContext(String index) {
        return getIndexContext(index, indexLookup.get(index).getPartNum());
    }

    private String prefixUserTableName(String table) {
        if (table.contains(".")) {
            return table;
        } else {
            return "tables." + table;
        }
    }

    // safely creates a row in information_schema.tables for tableName if none exists
    // (with isAllocated=false), and locks the table metadata row with the specified lock to
    // ensure no changes can be made until the current transaction commits.
    void lockTableMetadata(String tableName, LockType lockType) {
        // can't do this in one .compute() call, because we may need to block requesting
        // locks on the database/information_schema.tables, and a large part of tableInfoLookup
        // will be blocked while we're inside a compute call.
        boolean mayNeedToCreate = !tableInfoLookup.containsKey(tableName);
        if (mayNeedToCreate) {
            // TODO(proj4_part3): acquire all locks needed on database/information_schema.tables before compute()
            tableInfoLookup.compute(tableName, (tableName_, recordId) -> {
                if (recordId != null) { // record created between containsKey call and this
                    return recordId;
                }
                // should not block
                return Database.this.tableInfo.addRecord(new TableInfoRecord(tableName_).toDataBox());
            });
        }

        // TODO(proj4_part3): acquire all locks needed on the row in information_schema.tables
        LockContext context =
                getTableInfoContext().childContext(tableInfoLookup.get(tableName).getPageNum());
        LockUtil.ensureSufficientLockHeld(context, lockType);
    }

    private TableInfoRecord getTableMetadata(String tableName) {
        RecordId rid = tableInfoLookup.get(tableName);
        if (rid == null) {
            return new TableInfoRecord(tableName);
        }
        return new TableInfoRecord(tableInfo.getRecord(rid));
    }

    // safely creates a row in information_schema.indices for tableName,columnName if none exists
    // (with partNum=-1), and locks the index metadata row with the specified lock to ensure no
    // changes can be made until the current transaction commits.
    void lockIndexMetadata(String indexName, LockType lockType) {
        // see getTableMetadata - same logic/structure, just with a different table
        boolean mayNeedToCreate = !indexInfoLookup.containsKey(indexName);
        if (mayNeedToCreate) {
            // TODO(proj4_part3): acquire all locks needed on database/information_schema.indices before compute()
            indexInfoLookup.compute(indexName, (indexName_, recordId) -> {
                if (recordId != null) { // record created between containsKey call and this
                    return recordId;
                }
                String[] parts = indexName.split(",", 2);
                return Database.this.indexInfo.addRecord(Arrays.asList(
                            new StringDataBox(parts[0], 32),
                            new StringDataBox(parts[1], 32),
                            new IntDataBox(-1),
                            new IntDataBox(-1),
                            new LongDataBox(DiskSpaceManager.INVALID_PAGE_NUM),
                            new IntDataBox(TypeId.INT.ordinal()),
                            new IntDataBox(4),
                            new IntDataBox(-1)
                        ));
            });
        }

        // TODO(proj4_part3): acquire all locks needed on the row in information_schema.indices
        LockContext context =
                getIndexInfoContext().childContext(indexInfoLookup.get(indexName).getPageNum());
        LockUtil.ensureSufficientLockHeld(context, lockType);
    }

    private BPlusTreeMetadata getIndexMetadata(String tableName, String columnName) {
        String indexName = tableName + "," + columnName;
        RecordId rid = indexInfoLookup.get(indexName);
        if (rid == null) {
            return null;
        }
        return parseIndexMetadata(indexInfo.getRecord(rid));
    }

    /**
     * Start a new transaction.
     *
     * @return the new Transaction
     */
    public synchronized Transaction beginTransaction() {
        TransactionImpl t = new TransactionImpl(this.numTransactions, false);
        activeTransactions.register();
        if (activeTransactions.isTerminated()) {
            activeTransactions = new Phaser(1);
        }

        this.recoveryManager.startTransaction(t);
        ++this.numTransactions;
        return t;
    }

    /**
     * Start a transaction for recovery.
     *
     * @param transactionNum transaction number
     * @return the Transaction
     */
    private synchronized Transaction beginRecoveryTranscation(Long transactionNum) {
        this.numTransactions = Math.max(this.numTransactions, transactionNum + 1);

        TransactionImpl t = new TransactionImpl(transactionNum, true);
        activeTransactions.register();
        if (activeTransactions.isTerminated()) {
            activeTransactions = new Phaser(1);
        }

        return t;
    }

    /**
     * Gets the transaction number counter. This is the number of transactions that
     * have been created so far, and also the number of the next transaction to be created.
     */
    private synchronized long getTransactionCounter() {
        return this.numTransactions;
    }

    /**
     * Updates the transaction number counter.
     * @param newTransactionCounter new transaction number counter
     */
    private synchronized void setTransactionCounter(long newTransactionCounter) {
        this.numTransactions = newTransactionCounter;
    }

    private class TransactionContextImpl extends AbstractTransactionContext {
        long transNum;
        Map<String, String> aliases;
        Map<String, Table> tempTables;
        long tempTableCounter;

        private TransactionContextImpl(long tNum) {
            this.transNum = tNum;
            this.aliases = new HashMap<>();
            this.tempTables = new HashMap<>();
            this.tempTableCounter = 0;
        }

        @Override
        public long getTransNum() {
            return transNum;
        }

        @Override
        public int getWorkMemSize() {
            return Database.this.getWorkMem();
        }

        @Override
        public String createTempTable(Schema schema) {
            String tempTableName = "tempTable" + tempTableCounter++;
            String tableName = prefixTempTableName(tempTableName);

            int partNum = diskSpaceManager.allocPart();
            long pageNum = diskSpaceManager.allocPage(partNum);
            RecordId recordId = tableInfo.addRecord(Arrays.asList(
                    new StringDataBox(tableName, 32),
                    new IntDataBox(partNum),
                    new LongDataBox(pageNum),
                    new BoolDataBox(true),
                    new StringDataBox(new String(schema.toBytes()), MAX_SCHEMA_SIZE)));
            tableInfoLookup.put(tableName, recordId);

            LockContext lockContext = getTableContext(tableName, partNum);
            lockContext.disableChildLocks();
            HeapFile heapFile = new PageDirectory(bufferManager, partNum, pageNum, (short) 0, lockContext);
            tempTables.put(tempTableName, new Table(tableName, schema, heapFile, lockContext));
            tableLookup.put(tableName, tempTables.get(tempTableName));
            tableIndices.put(tableName, Collections.emptyList());

            return tempTableName;
        }

        private void deleteTempTable(String tempTableName) {
            if (!this.tempTables.containsKey(tempTableName)) {
                return;
            }

            String tableName = prefixTempTableName(tempTableName);
            RecordId recordId = tableInfoLookup.remove(tableName);
            Record record = tableInfo.deleteRecord(recordId);
            TableInfoRecord tableInfoRecord = new TableInfoRecord(record);
            bufferManager.freePart(tableInfoRecord.partNum);
            tempTables.remove(tempTableName);
            tableLookup.remove(tableName);
            tableIndices.remove(tableName);
        }

        @Override
        public void deleteAllTempTables() {
            Set<String> keys = new HashSet<>(tempTables.keySet());

            for (String tableName : keys) {
                deleteTempTable(tableName);
            }
        }

        @Override
        public void setAliasMap(Map<String, String> aliasMap) {
            this.aliases = new HashMap<>(aliasMap);
        }

        @Override
        public void clearAliasMap() {
            this.aliases.clear();
        }

        @Override
        public boolean indexExists(String tableName, String columnName) {
            try {
                resolveIndexFromName(tableName, columnName);
            } catch (DatabaseException e) {
                return false;
            }
            return true;
        }

        @Override
        public void updateIndexMetadata(BPlusTreeMetadata metadata) {
            indexInfo.updateRecord(Arrays.asList(
                                       new StringDataBox(metadata.getTableName(), 32),
                                       new StringDataBox(metadata.getColName(), 32),
                                       new IntDataBox(metadata.getOrder()),
                                       new IntDataBox(metadata.getPartNum()),
                                       new LongDataBox(metadata.getRootPageNum()),
                                       new IntDataBox(metadata.getKeySchema().getTypeId().ordinal()),
                                       new IntDataBox(metadata.getKeySchema().getSizeInBytes()),
                                       new IntDataBox(metadata.getHeight())
                                   ), indexInfoLookup.get(metadata.getName()));
        }

        @Override
        public Iterator<Record> sortedScan(String tableName, String columnName) {
            // TODO(proj4_part3): scan locking
            LockContext tableContext = getTableContext(tableName);
            LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
            Table tab = getTable(tableName);
            try {
                Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
                return new RecordIterator(tab, index.getSecond().scanAll());
            } catch (DatabaseException e1) {
                int offset = getTable(tableName).getSchema().getFieldNames().indexOf(columnName);
                try {
                    return new SortOperator(this, tableName,
                                            Comparator.comparing((Record r) -> r.getValues().get(offset))).iterator();
                } catch (QueryPlanException e2) {
                    throw new DatabaseException(e2);
                }
            }
        }

        @Override
        public Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue) {
            // TODO(proj4_part3): scan locking
            LockContext tableContext = getTableContext(tableName);
            LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
            Table tab = getTable(tableName);
            Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
            return new RecordIterator(tab, index.getSecond().scanGreaterEqual(startValue));
        }

        @Override
        public Iterator<Record> lookupKey(String tableName, String columnName, DataBox key) {
            Table tab = getTable(tableName);
            Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
            return new RecordIterator(tab, index.getSecond().scanEqual(key));
        }

        @Override
        public BacktrackingIterator<Record> getRecordIterator(String tableName) {
            return getTable(tableName).iterator();
        }

        @Override
        public BacktrackingIterator<Page> getPageIterator(String tableName) {
            return getTable(tableName).pageIterator();
        }

        @Override
        public BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block,
                int maxPages) {
            return getTable(tableName).blockIterator(block, maxPages);
        }

        @Override
        public boolean contains(String tableName, String columnName, DataBox key) {
            Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
            return index.getSecond().get(key).isPresent();
        }

        @Override
        public RecordId addRecord(String tableName, List<DataBox> values) {
            Table tab = getTable(tableName);
            RecordId rid = tab.addRecord(values);
            Schema s = tab.getSchema();
            List<String> colNames = s.getFieldNames();

            for (String indexName : tableIndices.get(tab.getName())) {
                String column = indexName.split(",")[1];
                resolveIndexFromName(tableName, column).getSecond().put(values.get(colNames.indexOf(column)), rid);
            }
            return rid;
        }

        @Override
        public RecordId deleteRecord(String tableName, RecordId rid) {
            Table tab = getTable(tableName);
            Schema s = tab.getSchema();

            Record rec = tab.deleteRecord(rid);
            List<DataBox> values = rec.getValues();
            List<String> colNames = s.getFieldNames();

            for (String indexName : tableIndices.get(tab.getName())) {
                String column = indexName.split(",")[1];
                resolveIndexFromName(tableName, column).getSecond().remove(values.get(colNames.indexOf(column)));
            }
            return rid;
        }

        @Override
        public Record getRecord(String tableName, RecordId rid) {
            return getTable(tableName).getRecord(rid);
        }

        @Override
        public RecordId updateRecord(String tableName, List<DataBox> values, RecordId rid) {
            Table tab = getTable(tableName);
            Schema s = tab.getSchema();

            Record rec = tab.updateRecord(values, rid);

            List<DataBox> oldValues = rec.getValues();
            List<String> colNames = s.getFieldNames();

            for (String indexName : tableIndices.get(tab.getName())) {
                String column = indexName.split(",")[1];
                int i = colNames.indexOf(column);
                BPlusTree tree = resolveIndexFromName(tableName, column).getSecond();
                tree.remove(oldValues.get(i));
                tree.put(values.get(i), rid);
            }
            return rid;
        }

        @Override
        public void runUpdateRecordWhere(String tableName, String targetColumnName,
                                         UnaryOperator<DataBox> targetValue,
                                         String predColumnName, PredicateOperator predOperator, DataBox predValue) {
            Table tab = getTable(tableName);
            Iterator<RecordId> recordIds = tab.ridIterator();

            Schema s = tab.getSchema();
            int uindex = s.getFieldNames().indexOf(targetColumnName);
            int pindex = s.getFieldNames().indexOf(predColumnName);

            while(recordIds.hasNext()) {
                RecordId curRID = recordIds.next();
                Record cur = getRecord(tableName, curRID);
                List<DataBox> recordCopy = new ArrayList<>(cur.getValues());

                if (predOperator == null || predOperator.evaluate(recordCopy.get(pindex), predValue)) {
                    recordCopy.set(uindex, targetValue.apply(recordCopy.get(uindex)));
                    updateRecord(tableName, recordCopy, curRID);
                }
            }
        }

        @Override
        public void runDeleteRecordWhere(String tableName, String predColumnName,
                                         PredicateOperator predOperator, DataBox predValue) {
            Table tab = getTable(tableName);
            Iterator<RecordId> recordIds = tab.ridIterator();

            Schema s = tab.getSchema();
            int pindex = s.getFieldNames().indexOf(predColumnName);

            while(recordIds.hasNext()) {
                RecordId curRID = recordIds.next();
                Record cur = getRecord(tableName, curRID);
                List<DataBox> recordCopy = new ArrayList<>(cur.getValues());

                if (predOperator == null || predOperator.evaluate(recordCopy.get(pindex), predValue)) {
                    deleteRecord(tableName, curRID);
                }
            }
        }

        @Override
        public Schema getSchema(String tableName) {
            return getTable(tableName).getSchema();
        }

        @Override
        public Schema getFullyQualifiedSchema(String tableName) {
            Schema schema = getTable(tableName).getSchema();
            List<String> newColumnNames = new ArrayList<>();
            for (String oldName : schema.getFieldNames()) {
                newColumnNames.add(tableName + "." + oldName);
            }

            return new Schema(newColumnNames, schema.getFieldTypes());
        }

        @Override
        public TableStats getStats(String tableName) {
            return getTable(tableName).getStats();
        }

        @Override
        public int getNumDataPages(String tableName) {
            return getTable(tableName).getNumDataPages();
        }

        @Override
        public int getNumEntriesPerPage(String tableName) {
            return getTable(tableName).getNumRecordsPerPage();
        }

        @Override
        public int getEntrySize(String tableName) {
            return getTable(tableName).getSchema().getSizeInBytes();
        }

        @Override
        public long getNumRecords(String tableName) {
            return getTable(tableName).getNumRecords();
        }

        @Override
        public int getTreeOrder(String tableName, String columnName) {
            return resolveIndexMetadataFromName(tableName, columnName).getSecond().getOrder();
        }

        @Override
        public int getTreeHeight(String tableName, String columnName) {
            return resolveIndexMetadataFromName(tableName, columnName).getSecond().getHeight();
        }

        @Override
        public void close() {
            // TODO(proj4_part3): release locks held by the transaction
            //TransactionContext transaction = TransactionContext.getTransaction();
            /*List<Lock> locks = lockManager.getLocks(this);
            List<LockContext> contexts = new ArrayList<>();
            for (Lock l : locks) {
                LockContext context = LockContext.fromResourceName(lockManager, l.name);
                contexts.add(context);
            }
            Collections.reverse(contexts);
            for (LockContext context : contexts) {
                context.release(this);
            }*/

            List<Lock> lockList = lockManager.getLocks(this);
            for (int i = lockList.size()-1;i >= 0;i--) {
                LockContext ctxt = LockContext.fromResourceName(lockManager,lockList.get(i).name);
                ctxt.release(this);
            }
            return;

        }

        @Override
        public String toString() {
            return "Transaction Context for Transaction " + transNum;
        }

        private Pair<String, BPlusTreeMetadata> resolveIndexMetadataFromName(String tableName,
                String columnName) {
            if (aliases.containsKey(tableName)) {
                tableName = aliases.get(tableName);
            }
            if (columnName.contains(".")) {
                String columnPrefix = columnName.split("\\.")[0];
                if (!tableName.equals(columnPrefix)) {
                    throw new DatabaseException("Column: " + columnName + " is not a column of " + tableName);
                }
                columnName = columnName.split("\\.")[1];
            }
            // remove tables. - index names do not use it
            if (tableName.startsWith("tables.")) {
                tableName = tableName.substring(tableName.indexOf(".") + 1);
            }
            String indexName = tableName + "," + columnName;

            // TODO(proj4_part3): add locking
            lockIndexMetadata(indexName, LockType.S);
            BPlusTreeMetadata metadata = getIndexMetadata(tableName, columnName);
            if (metadata == null) {
                throw new DatabaseException("no index with name " + indexName);
            }
            return new Pair<>(indexName, metadata);
        }

        private Pair<String, BPlusTree> resolveIndexFromName(String tableName,
                String columnName) {
            String indexName = resolveIndexMetadataFromName(tableName, columnName).getFirst();
            return new Pair<>(indexName, Database.this.indexLookup.get(indexName));
        }

        @Override
        public Table getTable(String tableName) {
            if (this.aliases.containsKey(tableName)) {
                tableName = this.aliases.get(tableName);
            }

            if (this.tempTables.containsKey(tableName)) {
                return this.tempTables.get(tableName);
            }

            if (!tableName.startsWith(METADATA_TABLE_PREFIX)) {
                tableName = prefixUserTableName(tableName);
            }

            // TODO(proj4_part3): add locking
            lockTableMetadata(tableName, LockType.S);
            TableInfoRecord record = getTableMetadata(tableName);
            if (!record.isAllocated()) {
                throw new DatabaseException("no table with name " + tableName);
            }
            return Database.this.tableLookup.get(tableName);
        }

        private String prefixTempTableName(String name) {
            String prefix = "temp." + transNum + "-";
            if (name.startsWith(prefix)) {
                return name;
            } else {
                return prefix + name;
            }
        }
    }

    private class TransactionImpl extends AbstractTransaction {
        private long transNum;
        private boolean recoveryTransaction;
        private TransactionContext transactionContext;

        private TransactionImpl(long transNum, boolean recovery) {
            this.transNum = transNum;
            this.recoveryTransaction = recovery;
            this.transactionContext = new TransactionContextImpl(transNum);
        }

        @Override
        protected void startCommit() {
            // TODO(proj5): replace immediate cleanup() call with job (the commented out code)

            transactionContext.deleteAllTempTables();

            recoveryManager.commit(transNum);

            this.cleanup();
            /*
            executor.execute(this::cleanup);
            */
        }

        @Override
        protected void startRollback() {
            executor.execute(() -> {
                recoveryManager.abort(transNum);
                this.cleanup();
            });
        }

        @Override
        public void cleanup() {
            if (getStatus() == Status.COMPLETE) {
                return;
            }

            if (!this.recoveryTransaction) {
                recoveryManager.end(transNum);
            }

            transactionContext.close();
            activeTransactions.arriveAndDeregister();
        }

        @Override
        public long getTransNum() {
            return transNum;
        }

        @Override
        public void createTable(Schema s, String tableName) {
            if (tableName.contains(".") && !tableName.startsWith("tables.")) {
                throw new IllegalArgumentException("name of new table may not contain '.'");
            }

            String prefixedTableName = prefixUserTableName(tableName);
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(proj4_part3): add locking

                lockTableMetadata(prefixedTableName, LockType.X);

                TableInfoRecord record = getTableMetadata(prefixedTableName);
                if (record.isAllocated()) {
                    throw new DatabaseException("table " + prefixedTableName + " already exists");
                }

                record.partNum = diskSpaceManager.allocPart();
                record.pageNum = diskSpaceManager.allocPage(record.partNum);
                record.isTemporary = false;
                record.schema = s;
                tableInfo.updateRecord(record.toDataBox(), tableInfoLookup.get(prefixedTableName));

                LockContext tableContext = getTableContext(prefixedTableName, record.partNum);
                HeapFile heapFile = new PageDirectory(bufferManager, record.partNum, record.pageNum,
                                                      (short) 0, tableContext);
                tableLookup.put(prefixedTableName, new Table(prefixedTableName, s,
                                heapFile, tableContext));
                tableIndices.put(prefixedTableName, new ArrayList<>());
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropTable(String tableName) {
            if (tableName.contains(".") && !tableName.startsWith("tables.")) {
                throw new IllegalArgumentException("name of table may not contain '.': " + tableName);
            }

            String prefixedTableName = prefixUserTableName(tableName);
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(proj4_part3): add locking

                lockTableMetadata(prefixedTableName, LockType.X);
                LockContext tableContext = getTableContext(tableName);
                LockUtil.ensureSufficientLockHeld(tableContext, LockType.X);

                TableInfoRecord record = getTableMetadata(prefixedTableName);
                if (!record.isAllocated()) {
                    throw new DatabaseException("table " + prefixedTableName + " does not exist");
                }

                for (String indexName : new ArrayList<>(tableIndices.get(prefixedTableName))) {
                    String[] parts = indexName.split(",");
                    dropIndex(parts[0], parts[1]);
                }

                RecordId tableRecordId = tableInfoLookup.get(prefixedTableName);
                tableInfo.updateRecord(new TableInfoRecord(prefixedTableName).toDataBox(), tableRecordId);

                tableIndices.remove(prefixedTableName);
                tableLookup.remove(prefixedTableName);
                bufferManager.freePart(record.partNum);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropAllTables() {
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(proj4_part3): add locking
                LockUtil.ensureSufficientLockHeld(getTableInfoContext().parentContext(),LockType.X);
                List<String> tableNames = new ArrayList<>(tableLookup.keySet());

                for (String s : tableNames) {
                    if (s.startsWith("tables.")) {
                        this.dropTable(s);
                    }
                }
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void createIndex(String tableName, String columnName, boolean bulkLoad) {
            if (tableName.contains(".") && !tableName.startsWith("tables.")) {
                throw new IllegalArgumentException("name of table may not contain '.'");
            }
            String prefixedTableName = prefixUserTableName(tableName);
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(proj4_part3): add locking

                lockTableMetadata(prefixedTableName, LockType.S);
                TableInfoRecord tableMetadata = getTableMetadata(prefixedTableName);

                if (!tableMetadata.isAllocated()) {
                    throw new DatabaseException("table " + tableName + " does not exist");
                }

                Schema s = tableMetadata.schema;
                List<String> schemaColNames = s.getFieldNames();
                List<Type> schemaColType = s.getFieldTypes();
                if (!schemaColNames.contains(columnName)) {
                    throw new DatabaseException("table " + tableName + " does not have a column " + columnName);
                }

                int columnIndex = schemaColNames.indexOf(columnName);
                Type colType = schemaColType.get(columnIndex);
                String indexName = tableName + "," + columnName;

                lockIndexMetadata(indexName, LockType.X);

                BPlusTreeMetadata metadata = getIndexMetadata(tableName, columnName);
                if (metadata != null) {
                    throw new DatabaseException("index already exists on " + tableName + "(" + columnName + ")");
                }

                int order = BPlusTree.maxOrder(BufferManager.EFFECTIVE_PAGE_SIZE, colType);
                List<DataBox> values = Arrays.asList(
                                           new StringDataBox(tableName, 32),
                                           new StringDataBox(columnName, 32),
                                           new IntDataBox(order),
                                           new IntDataBox(diskSpaceManager.allocPart()),
                                           new LongDataBox(DiskSpaceManager.INVALID_PAGE_NUM),
                                           new IntDataBox(colType.getTypeId().ordinal()),
                                           new IntDataBox(colType.getSizeInBytes()),
                                           new IntDataBox(-1)
                                       );
                indexInfo.updateRecord(values, indexInfoLookup.get(indexName));
                metadata = parseIndexMetadata(new Record(values));
                assert (metadata != null);

                LockContext indexContext = getIndexContext(indexName, metadata.getPartNum());
                indexLookup.put(indexName, new BPlusTree(bufferManager, metadata, indexContext));
                tableIndices.get(prefixedTableName).add(indexName);

                // load data into index
                Table table = tableLookup.get(prefixedTableName);
                BPlusTree tree = indexLookup.get(indexName);
                if (bulkLoad) {
                    throw new UnsupportedOperationException("not implemented");
                } else {
                    for (RecordId rid : (Iterable<RecordId>) table::ridIterator) {
                        Record record = table.getRecord(rid);
                        tree.put(record.getValues().get(columnIndex), rid);
                    }
                }
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropIndex(String tableName, String columnName) {
            String prefixedTableName = prefixUserTableName(tableName);
            String indexName = tableName + "," + columnName;
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(proj4_part3): add locking

                lockIndexMetadata(indexName, LockType.X);
                LockUtil.ensureSufficientLockHeld(getIndexContext(indexName),LockType.X);
                BPlusTreeMetadata metadata = getIndexMetadata(tableName, columnName);
                if (metadata == null) {
                    throw new DatabaseException("no index on " + tableName + "(" + columnName + ")");
                }
                indexInfo.updateRecord(Arrays.asList(
                                           new StringDataBox(tableName, 32),
                                           new StringDataBox(columnName, 32),
                                           new IntDataBox(-1),
                                           new IntDataBox(-1),
                                           new LongDataBox(DiskSpaceManager.INVALID_PAGE_NUM),
                                           new IntDataBox(TypeId.INT.ordinal()),
                                           new IntDataBox(4),
                                           new IntDataBox(-1)
                                       ), indexInfoLookup.get(indexName));

                bufferManager.freePart(metadata.getPartNum());
                indexLookup.remove(indexName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public QueryPlan query(String tableName) {
            return new QueryPlan(transactionContext, tableName);
        }

        @Override
        public QueryPlan query(String tableName, String alias) {
            return new QueryPlan(transactionContext, tableName, alias);
        }

        @Override
        public void insert(String tableName, List<DataBox> values) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.addRecord(tableName, values);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue) {
            update(tableName, targetColumnName, targetValue, null, null, null);
        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue,
                           String predColumnName, PredicateOperator predOperator, DataBox predValue) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.runUpdateRecordWhere(tableName, targetColumnName, targetValue, predColumnName,
                                                        predOperator, predValue);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void delete(String tableName, String predColumnName, PredicateOperator predOperator,
                           DataBox predValue) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.runDeleteRecordWhere(tableName, predColumnName, predOperator, predValue);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void savepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.savepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void rollbackToSavepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.rollbackToSavepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void releaseSavepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.releaseSavepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public Schema getSchema(String tableName) {
            return transactionContext.getSchema(tableName);
        }

        @Override
        public Schema getFullyQualifiedSchema(String tableName) {
            return transactionContext.getSchema(tableName);
        }

        @Override
        public TableStats getStats(String tableName) {
            return transactionContext.getStats(tableName);
        }

        @Override
        public int getNumDataPages(String tableName) {
            return transactionContext.getNumDataPages(tableName);
        }

        @Override
        public int getNumEntriesPerPage(String tableName) {
            return transactionContext.getNumEntriesPerPage(tableName);
        }

        @Override
        public int getEntrySize(String tableName) {
            return transactionContext.getEntrySize(tableName);
        }

        @Override
        public long getNumRecords(String tableName) {
            return transactionContext.getNumRecords(tableName);
        }

        @Override
        public int getTreeOrder(String tableName, String columnName) {
            return transactionContext.getTreeOrder(tableName, columnName);
        }

        @Override
        public int getTreeHeight(String tableName, String columnName) {
            return transactionContext.getTreeHeight(tableName, columnName);
        }

        @Override
        public TransactionContext getTransactionContext() {
            return transactionContext;
        }

        @Override
        public String toString() {
            return "Transaction " + transNum + " (" + getStatus().toString() + ")";
        }
    }
}
