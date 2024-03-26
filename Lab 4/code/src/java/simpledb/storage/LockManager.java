package simpledb.storage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.DeadlockException;

import simpledb.transaction.*;

public class LockManager {
    private class Lock {
        TransactionId tid;
        // false for shared lock or true for exclusive lock
        boolean lockType;

        public Lock(TransactionId tid, boolean lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    // private class DependencyGraph {
    //     private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> graph;

    //     public DependencyGraph() {
    //         graph = new ConcurrentHashMap<>();
    //     }

    //     public void addNode(TransactionId tid) {
    //         if (graph.containsKey(tid)) {
    //             return;
    //         }
    //         graph.put(tid, new HashSet<>());
    //     }

    //     public void addEdge(TransactionId from, TransactionId to) {
    //         addNode(from);
    //         addNode(to);
    //         graph.get(from).add(to);
    //     }

    //     public void removeNode(TransactionId tid) {
    //         for (TransactionId tidToRemove : graph.keySet()) {
    //             HashSet<TransactionId> transactions = graph.get(tidToRemove);
    //             transactions.remove(tid);
    //         }
    //         graph.remove(tid);
    //     }

    //     public void removeEdgy(TransactionId from, TransactionId to) {
    //         if (graph.containsKey(from) && graph.containsKey(to)) {
    //             graph.get(from).remove(to);
    //         }
    //     }

    //     public boolean isCyclic() {
    //         ConcurrentHashMap<TransactionId, Boolean> visited = new ConcurrentHashMap<>();
    //         ConcurrentHashMap<TransactionId, Boolean> path = new ConcurrentHashMap<>();
    //         for (TransactionId tid : graph.keySet()) {
    //             if (dfs(tid, visited, path)) {
    //                 return true;
    //             }
    //         }
    //         return false;
    //     }

    //     private boolean dfs(TransactionId tid,
    //                         ConcurrentHashMap<TransactionId, Boolean> visited,
    //                         ConcurrentHashMap<TransactionId, Boolean> path) {
    //         if (path.getOrDefault(tid, false)) {
    //             return false;
    //         }
    //         if (visited.getOrDefault(tid, false)) {
    //             return true;
    //         }
    //         visited.put(tid, true);
    //         path.put(tid, true);
    //         HashSet<TransactionId> transactions = graph.get(tid);
    //         for (TransactionId tidToTraverse : transactions) {
    //             if (dfs(tidToTraverse, visited, path)) {
    //                 return true;
    //             }
    //         }
    //         path.put(tid, false);
    //         return false;
    //     }
    // }

    ConcurrentHashMap<PageId, ArrayList<Lock>> lockMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquireLock(TransactionId tid, PageId pid, boolean lockType) {
        if (!lockMap.containsKey(pid)) {
            Lock lock = new Lock(tid, lockType);
            ArrayList<Lock> locks = new ArrayList<>();
            locks.add(lock);
            lockMap.put(pid, locks);
            return true;
        }

        ArrayList<Lock> locks = lockMap.get(pid);
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if (lock.tid.equals(tid)) {
                if (lock.lockType == lockType || lock.lockType) {
                    return true;
                }
                if (locks.size() == 1) {
                    lock.lockType = true;
                    return true;
                } else {
                    return false;
                }
            }
        }

        if (locks.size() == 1 && locks.get(0).lockType) {
            return false;
        }

        if (!lockType) {
            Lock lock = new Lock(tid, false);
            locks.add(lock);
            lockMap.put(pid, locks);
            return true;
        }
        return false;
    }

    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        if (!lockMap.containsKey(pid)) {
            return false;
        }

        ArrayList<Lock> locks = lockMap.get(pid);
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if (lock.tid.equals(tid)) {
                locks.remove(i);
                if (locks.size() == 0) {
                    lockMap.remove(pid);
                }
                return true;
            }
        }

        return false;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        if (!lockMap.containsKey(pid)) {
            return false;
        }
        ArrayList<Lock> locks = lockMap.get(pid);
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if (lock.tid.equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
