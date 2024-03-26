package simpledb.execution;

import simpledb.transaction.TransactionId;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.common.Type;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int tableId;
    private TransactionId tId;
    private boolean isCalled;
    
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
    	if (!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())) {
    		throw new DbException("The TupleDesc of child differs from inserted table");
    	}
    	this.child = child;
    	this.tableId = tableId;
    	tId = t;
    	isCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
    	child.open();
    	super.open();
    	
    }

    public void close() {
    	child.close();
    	super.close();
    	isCalled = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (isCalled) {
    		return null;
    	}
    	isCalled = true;
    	int count = 0;
    	while (child.hasNext()) {
    		Tuple childTuple = child.next();
    		try {
    			Database.getBufferPool().insertTuple(tId, tableId, childTuple);
    			count += 1;
    		} catch (Exception e) {
    			throw new DbException("Cannot insert " + childTuple.toString() + " to table " + Integer.toString(tableId));
    		}
    	}
    	Tuple countTuple = new Tuple(getTupleDesc());
    	countTuple.setField(0, new IntField(count));
    	return countTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	child = children[0];
    }
}
