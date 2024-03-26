package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;
import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private OpIterator child;
    private Aggregator.Op aggOperator;
    private int groupbyField;
    private int aggField;
    private Type groupbyFieldType;
    private Aggregator aggregator;
    private OpIterator tuples;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    	this.child = child;
    	aggOperator = aop;
    	aggField = afield;
    	groupbyField = gfield;
    	groupbyFieldType = groupbyField != Aggregator.NO_GROUPING ? child.getTupleDesc().getFieldType(groupbyField) : null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
    	return groupbyField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        return groupbyField != Aggregator.NO_GROUPING ? child.getTupleDesc().getFieldName(groupbyField) : null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(aggField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aggOperator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
    	child.open();
    	super.open();
    	
    	Type aggType = child.getTupleDesc().getFieldType(aggField);
    	if (aggType == Type.INT_TYPE) {
    		aggregator = new IntegerAggregator(groupbyField, groupbyFieldType, aggField, aggOperator);
    	} else if (aggType == Type.STRING_TYPE) {
    		aggregator = new StringAggregator(groupbyField, groupbyFieldType, aggField, aggOperator);
    	}
    	while (child.hasNext()) {
    		Tuple tuple = child.next();
    		aggregator.mergeTupleIntoGroup(tuple);
    	}
    	tuples = aggregator.iterator();
    	tuples.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	return tuples.hasNext() ? tuples.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc td;
    	Type aggFieldType = child.getTupleDesc().getFieldType(aggField);
    	String aggFieldName = child.getTupleDesc().getFieldName(aggField);
    	if (groupbyField != Aggregator.NO_GROUPING) {
    		String groupbyFieldName = child.getTupleDesc().getFieldName(groupbyField);
    		return new TupleDesc(new Type[] {groupbyFieldType, aggFieldType}, new String[] {groupbyFieldName, aggFieldName});
    	} else {
    		return new TupleDesc(new Type[] {aggFieldType}, new String[] {aggFieldName});
    	}
    }

    public void close() {
    	child.close();
    	super.close();
    	tuples.close();
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
