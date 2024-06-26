package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid;
    // a hack to remember the last page that had a free slot
    private volatile int lastEmptyPage = -1;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId id = (HeapPageId) pid;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            byte[] pageBuf = new byte[BufferPool.getPageSize()];
            if (bis.skip((long) id.getPageNumber() * BufferPool.getPageSize()) != (long) id
                    .getPageNumber() * BufferPool.getPageSize()) {
                throw new IllegalArgumentException(
                        "Unable to seek to correct place in heapfile");
            }
            int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
            if (retval == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (retval < BufferPool.getPageSize()) {
                throw new IllegalArgumentException("Unable to read "
                        + BufferPool.getPageSize() + " bytes from heapfile");
            }
            Debug.log(1, "HeapFile.readPage: read page %d", id.getPageNumber());
            return new HeapPage(id, pageBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Close the file on success or error
        // Ignore failures closing the file
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        HeapPage p = (HeapPage) page;
        // System.out.println("Writing back page " + p.getId().pageno());
        byte[] data = p.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek((long) p.getId().getPageNumber() * BufferPool.getPageSize());
        rf.write(data);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // XXX: this seems to be rounding it down. isn't that wrong?
        // XXX: (marcua) no - we only ever write full pages
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> dirtypages = new ArrayList<>();

        // find the first page with a free slot in it
        int i = 0;
        if (lastEmptyPage != -1)
            i = lastEmptyPage;
        // XXX: Would it not be better to scan from numPages() to 0 since the
        // last pages are more likely to have empty slots?
        for (; i < numPages(); i++) {
            Debug.log(
                    4,
                    "HeapFile.addTuple: checking free slots on page %d of table %d",
                    i, tableid);
            HeapPageId pid = new HeapPageId(tableid, i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid,
                    Permissions.READ_WRITE);

            // no empty slots
            //
            // think about why we have to invoke releasePage here.
            // can you think of ways where
            if (p.getNumUnusedSlots() == 0) {
                Debug.log(
                        4,
                        "HeapFile.addTuple: no free slots on page %d of table %d",
                        i, tableid);
                Database.getBufferPool().unsafeReleasePage(tid, pid);
                // we mistakenly got here through lastEmptyPage, just add a page
                // XXX we know this isn't very pretty.
                if (lastEmptyPage != -1) {
                    lastEmptyPage = -1;
                    break;
                }
                continue;
            }
            Debug.log(4, "HeapFile.addTuple: %d free slots in table %d",
                    p.getNumUnusedSlots(), tableid);
            p.insertTuple(t);
            lastEmptyPage = p.getId().getPageNumber();
            // System.out.println("nfetches = " + nfetches);
            dirtypages.add(p);
            return dirtypages;
        }

        // no empty slots -- append a page
        // This must be synchronized so that the append operation is atomic.
        // Otherwise a second
        // thread could be blocked just after opening the file. The first
        // transaction flushes
        // new tuples to the page. The second transaction then overwrites the
        // data with an empty
        // page, losing the new data.
        synchronized (this) {
            BufferedOutputStream bw = new BufferedOutputStream(
                    new FileOutputStream(f, true));
            byte[] emptyData = HeapPage.createEmptyPageData();
            bw.write(emptyData);
            bw.close();
        }

        // by virtue of writing these bits to the HeapFile, it is now visible.
        // so some other dude may have obtained a read lock on the empty page
        // we just created---which is ok, we haven't yet added the tuple.
        // we just need to lock the page before we can add the tuple to it.

        HeapPage p = (HeapPage) Database.getBufferPool()
                .getPage(tid, new HeapPageId(tableid, numPages() - 1),
                        Permissions.READ_WRITE);
        p.insertTuple(t);
        lastEmptyPage = p.getId().getPageNumber();
        // System.out.println("nfetches = " + nfetches);
        dirtypages.add(p);
        return dirtypages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(
                tid,
                new HeapPageId(tableid, t.getRecordId().getPageId()
                        .getPageNumber()), Permissions.READ_WRITE);
        p.deleteTuple(t);
        List<Page> pages = new ArrayList<>();
        pages.add(p);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    int curpgno = 0;

    final TransactionId tid;
    final HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() {
        curpgno = -1;
    }

    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                    curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext())
                it = null;
        }

        if (it == null)
            return null;
        return it.next();
    }

    public void rewind() {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}
