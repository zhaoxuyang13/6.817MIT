package simpledb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Random;
import java.util.Vector;

class LockManager {
    private final Object[] mutexes;
    private final List<Set<TransactionId>> readlock_holders; 
    private final List<TransactionId> writelock_holders;
    private final int[] waiters;
    private final Random random = new Random();
    private final int MIN_TIME = 100, MAX_TIME = 1000;

    public LockManager(int numPages){
        mutexes = new Object[numPages];
        readlock_holders = new Vector<>();
        writelock_holders = new Vector<>();
        waiters = new int[numPages];
        for(int i = 0; i < numPages; i ++){
            mutexes[i] = new Object();
            readlock_holders.add(new HashSet<>());
            writelock_holders.add(null);
        }
    }

    private boolean holdsReadLock(TransactionId tid, int index){
        synchronized (mutexes[index]){
            return readlock_holders.get(index).contains(tid);
        }
    }

    private boolean holdsWriteLock(TransactionId tid, int index){
        synchronized(mutexes[index]){
            return tid.equals(writelock_holders.get(index));
        }
    }
    public boolean isHolding(TransactionId tid, int index){
        return holdsWriteLock(tid, index) || holdsReadLock(tid, index); // what if cut in the middle
    }
    private void acquireReadLock(TransactionId tid, int index) throws InterruptedException {
        if(!isHolding(tid, index)){
            synchronized(mutexes[index]){
                final Thread thread = Thread.currentThread();
                final Timer timer  = new Timer(true);
                timer.schedule(new TimerTask(){
                    @Override
                    public void run() {
                        thread.interrupt();
                    }
                }, MIN_TIME + random.nextInt(MAX_TIME - MIN_TIME));// self wake up .
                while(waiters[index] != 0){
                    mutexes[index].wait();
                }
                timer.cancel();
                readlock_holders.get(index).add(tid);
            }
        }
    }
    private boolean releaseReadLock(TransactionId tid, int index) {
        if(!isHolding(tid, index)){
            return false;
        }else {
            synchronized(mutexes[index]){
                readlock_holders.get(index).remove(tid);
                if(readlock_holders.get(index).isEmpty()){
                    mutexes[index].notifyAll();
                }
                
            }
            return true;
        }
    }
    private boolean hasOtherReader(TransactionId tid, int index){
        synchronized(mutexes[index]){
            for(TransactionId id : readlock_holders.get(index)){
                if(!id.equals(tid)){
                    return true;
                }
            }
            return false;   
        }
    }
    private void acquireWriteLokc(TransactionId tid, int index) throws InterruptedException {
        if(!holdsWriteLock(tid, index)){
            synchronized(mutexes[index]){
                final Thread thread = Thread.currentThread();
				final Timer timer = new Timer(true);
				waiters[index]++;
				timer.schedule(new TimerTask() {
					@Override public void run() {
						thread.interrupt();
					}
                }, MIN_TIME+random.nextInt(MAX_TIME-MIN_TIME));
                while(hasOtherReader(tid, index) || writelock_holders.get(index) != null){
                    mutexes[index].wait();
                }
                readlock_holders.get(index).remove(tid);
                writelock_holders.set(index, tid);
                timer.cancel(); // why after set.
            }
        }
    }
    private boolean releaseWriteLock(TransactionId tid,int index){
        if(!holdsWriteLock(tid, index)){
            return false;
        }else {
            synchronized(mutexes[index]){
                writelock_holders.set(index, null);
                waiters[index]--;
                mutexes[index].notifyAll();
            }
            return true;
        }
    }
    public void acquire(TransactionId tid, int index, Permissions perm) throws InterruptedException {
        try{
            if(perm.equals(Permissions.READ_ONLY)){
                acquireReadLock(tid, index);
            }else {
                acquireWriteLokc(tid, index);
            }
            
        }catch(InterruptedException e){
            for(int j = 0; j < mutexes.length; j ++){
                release(tid,j);
            }
            if(perm.equals(Permissions.READ_WRITE)){
                synchronized(mutexes[index]){
                    waiters[index] --;
                }
            }
            throw new InterruptedException("dead lock detected");
        }
    }
    public boolean release(TransactionId tid, int index){
        return releaseWriteLock(tid, index) || releaseReadLock(tid, index);
    }
}