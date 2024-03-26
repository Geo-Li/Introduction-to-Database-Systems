package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.storage.DbFileIterator;
import simpledb.storage.HeapPage;
import simpledb.storage.HeapPageId;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int groupbyField;
    private Type groupbyFieldType;
    private int aggregateField;
    private Op operator;
    private HashMap<Field, AggregatorInfo> gbAggMap = new HashMap<Field, AggregatorInfo>();
    private AggregatorInfo no_grpInfo = new AggregatorInfo();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	groupbyField = gbfield;
    	groupbyFieldType = gbfieldtype;
    	aggregateField = afield;
    	operator = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	int aggVal = ((IntField) tup.getField(aggregateField)).getValue();
    	if (groupbyField == NO_GROUPING) {
    		no_grpInfo.count += 1;
    		no_grpInfo.maxVal = Math.max(aggVal, no_grpInfo.maxVal);
    		no_grpInfo.minVal = Math.min(aggVal, no_grpInfo.minVal);
    		no_grpInfo.sum += aggVal;
    	} else {
    		Field gbField = tup.getField(groupbyField);
    		if (!gbAggMap.containsKey(gbField)) {
    			gbAggMap.put(gbField, new AggregatorInfo());
    		}
    		AggregatorInfo aggInfo = gbAggMap.get(gbField);
    		aggInfo.count += 1;
    		aggInfo.maxVal = Math.max(aggVal, aggInfo.maxVal);
    		aggInfo.minVal = Math.min(aggVal, aggInfo.minVal);
    		aggInfo.sum += aggVal;
    	}    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc td;
    	if (groupbyField == NO_GROUPING) {
    		td = new TupleDesc(new Type[] {Type.INT_TYPE});
    		Tuple tuple = new Tuple(td);
    		switch (operator) {
    		case MIN:
    			tuple.setField(0, new IntField(no_grpInfo.minVal));
    			tuples.add(tuple);
    			break;
    		case MAX:
    			tuple.setField(0, new IntField(no_grpInfo.maxVal));
    			tuples.add(tuple);
    			break;
    		case SUM:
    			tuple.setField(0, new IntField(no_grpInfo.sum));
    			tuples.add(tuple);
    			break;
    		case AVG:
    			tuple.setField(0, new IntField(no_grpInfo.sum/no_grpInfo.count));
    			tuples.add(tuple);
    			break;
    		case COUNT:
    			tuple.setField(0, new IntField(no_grpInfo.count));
    			tuples.add(tuple);
    			break;
    		}
    	} else {
    		td = new TupleDesc(new Type[] {groupbyFieldType, Type.INT_TYPE});
    		for (Field gbField : gbAggMap.keySet()) {
    			Tuple tuple = new Tuple(td);
    			tuple.setField(0, gbField);
    			switch (operator) {
        		case MIN:
        			tuple.setField(1, new IntField(gbAggMap.get(gbField).minVal));
        			tuples.add(tuple);
        			break;
        		case MAX:
        			tuple.setField(1, new IntField(gbAggMap.get(gbField).maxVal));
        			tuples.add(tuple);
        			break;
        		case SUM:
        			tuple.setField(1, new IntField(gbAggMap.get(gbField).sum));
        			tuples.add(tuple);
        			break;
        		case AVG:
        			tuple.setField(1, new IntField(gbAggMap.get(gbField).sum/gbAggMap.get(gbField).count));
        			tuples.add(tuple);
        			break;
        		case COUNT:
        			tuple.setField(1, new IntField(gbAggMap.get(gbField).count));
        			tuples.add(tuple);
        			break;
        		}
    		}
    	}
    	return new TupleIterator(td, tuples);
    }
    
    private class AggregatorInfo {
		public int count;
    	public int maxVal;
    	public int minVal;
    	public int sum;
    	
    	public AggregatorInfo() {
    		count = 0;
    		maxVal = 0;
    		minVal = (int) Double.POSITIVE_INFINITY;
    		sum = 0;
    	}
	}
}
