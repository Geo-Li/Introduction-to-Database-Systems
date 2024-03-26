package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.lang.Math;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file;
	private RandomAccessFile heapFile;
	private TupleDesc tupleDesc;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	file = f;
    	tupleDesc = td;
    	try {
    		heapFile = new RandomAccessFile(file, "r");
    	} catch (Exception e) {
    		
    	}
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }
    

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	try {
        	RandomAccessFile heapFile = new RandomAccessFile(file, "r");
        	int pageNumber = pid.getPageNumber();
        	// set the space for the new page
        	int pageSize = Database.getBufferPool().getPageSize();
        	byte[] content = new byte[pageSize];
        	heapFile.seek(pageSize * pageNumber);
        	heapFile.read(content);
        	HeapPage page = new HeapPage((HeapPageId)pid, content);
        	heapFile.close();
        	return page;
    	} catch (Exception e) {
    		return null;
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	try {
    		return (int)Math.ceil(heapFile.length() / Database.getBufferPool().getPageSize());
    	} catch (Exception e) {
    		return 0;
    	}
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new DbFileIterator() {
    		private Iterator<Tuple> tuples;
    		private int pageNumber = -1;
    		
    		@Override
            public void open() throws DbException, TransactionAbortedException {
    			pageNumber = 0;
    			tuples = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNumber), Permissions.READ_ONLY)).iterator();
    		}
    		
    		@Override
    		public boolean hasNext()
    		        throws DbException, TransactionAbortedException {
    			// Check if there is one more page to read
    			if (pageNumber < 0) {
    				return false;
    			}
    			if (tuples.hasNext())
    			{
    				return true;
    			}
    			while (!tuples.hasNext()) {
    				if (pageNumber >= numPages() - 1) {
    					return false;
    				}
    				pageNumber += 1;
    				tuples = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNumber), Permissions.READ_ONLY)).iterator();
    			}
    			return true;
    		}

    		@Override
		    public Tuple next()
		        throws DbException, TransactionAbortedException, NoSuchElementException {
    			if (pageNumber < 0) {
    				throw new NoSuchElementException();
    			}
    			return tuples.next();
    		}

    		@Override
		    public void rewind() throws DbException, TransactionAbortedException {
    			open();
    		}

    		@Override
		    public void close() {
    			pageNumber = -1;
    		}
    	};
    }
}

