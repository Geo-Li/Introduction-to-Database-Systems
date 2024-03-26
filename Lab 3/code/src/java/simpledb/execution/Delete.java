package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tId;
    private OpIterator child;
    private boolean isCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
    	tId = t;
    	this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
    			Database.getBufferPool().deleteTuple(tId, childTuple);
        		count += 1;
    		} catch (Exception e) {
    			throw new DbException("Cannot delete " + childTuple.toString());
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
