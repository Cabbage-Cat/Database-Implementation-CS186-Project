package edu.berkeley.cs186.database.recovery;

import java.util.Iterator;

public interface LogManager extends Iterable<LogRecord>, AutoCloseable {
    /**
     * Writes to the first record in the log.
     * @param record log record to replace first record with
     */
    void rewriteMasterRecord(MasterLogRecord record);

    /**
     * Appends a log record to the log.
     * @param record log record to append to the log
     * @return LSN of new log record
     */
    long appendToLog(LogRecord record);

    /**
     * Fetches a specific log record.
     * @param LSN LSN of record to fetch
     * @return log record with the specified LSN or null if no record found
     */
    LogRecord fetchLogRecord(long LSN);

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     * @param LSN LSN up to which the log should be flushed
     */
    void flushToLSN(long LSN);

    /**
     * @return flushedLSN
     */
    long getFlushedLSN();

    /**
     * Scan forward in the log from LSN.
     * @param LSN LSN to start scanning from
     * @return iterator over log entries from LSN
     */
    Iterator<LogRecord> scanFrom(long LSN);

    /**
     * Prints the entire log. For debugging uses only.
     */
    void print();

    /**
     * Scan forward in the log from the first record.
     * @return iterator over all log entries
     */
    @Override
    Iterator<LogRecord> iterator();

    @Override
    void close();
}
