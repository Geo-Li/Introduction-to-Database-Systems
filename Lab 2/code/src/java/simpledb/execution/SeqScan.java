package simpledb.execution;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.*;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private int tableId;
    private String tableAlias;
    private DbFileIterator tuples;
    private TransactionId tid;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	tuples = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);    	
    	this.tid = tid;
    	tableId = tableid;
    	if (tableAlias == null) {
    		tableAlias = "null";
    	}
    	this.tableAlias = tableAlias;
    }
    
    public TransactionId getTID() {
    	return tid;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
    	tableId = tableid;
    	if (tableAlias == null) {
    		tableAlias = "null";
    	}
    	this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
    	tuples.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc tupleDesc = Database.getCatalog().getDatabaseFile(tableId).getTupleDesc();
    	Type[] tupleTypes = new Type[tupleDesc.numFields()];
    	String[] updatedTupleFields = new String[tupleDesc.numFields()];
    	for (int i = 0; i < tupleDesc.numFields(); i++) {
    		String newField = tupleDesc.getFieldName(i);
    		if (tupleDesc.getFieldName(i) == null || tupleDesc.getFieldName(i).equals("")) {
    			newField = "null";
    		}
    		updatedTupleFields[i] = tableAlias + "." + newField;
    		tupleTypes[i] = tupleDesc.getFieldType(i);
    	}
        return new TupleDesc(tupleTypes, updatedTupleFields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return tuples.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return tuples.next();
    }

    public void close() {
    	tuples.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	tuples.rewind();
    }
}
