/**
 *  Copyright 2003-2010 Terracotta, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.sf.ehcache.store.disk;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.concurrent.ConcurrencyUtil;
import net.sf.ehcache.event.RegisteredEventListeners;
import net.sf.ehcache.store.disk.ods.FileAllocationTree;
import net.sf.ehcache.store.disk.ods.Region;
import net.sf.ehcache.transaction.SoftLock;
import net.sf.ehcache.util.MemoryEfficientByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.modules.sizeof.filter.IgnoreSizeOf;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A mock-up of a on-disk element proxy factory.
 *
 * @author Chris Dennis
 * @author Ludovic Orban
 */
@IgnoreSizeOf
public class DiskStorageFactory {

    /**
     * Path stub used to create unique ehcache directories.
     */
    protected static final String AUTO_DISK_PATH_DIRECTORY_PREFIX = "ehcache_auto_created";
    private static final int SERIALIZATION_CONCURRENCY_DELAY = 250;
    private static final int SHUTDOWN_GRACE_PERIOD = 60;
    private static final int MEGABYTE = 1024 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(DiskStorageFactory.class.getName());

    /**
     * The store bound to this factory.
     */
    protected volatile DiskStore                  store;

    private final BlockingQueue<Runnable> diskQueue;
    /**
     * Executor service used to write elements to disk
     */
    private final ScheduledThreadPoolExecutor diskWriter;

    private final long queueCapacity;

    private final File             file;
    private final RandomAccessFile[] dataAccess;

    private final FileAllocationTree allocator;

    private final RegisteredEventListeners eventService;

    private volatile int elementSize;

    private static final int MAX_EVICT = 5;
    private static final int SAMPLE_SIZE = 30;

    private final ElementSubstituteFilter onDiskFilter = new OnDiskFilter();

    private final AtomicInteger onDisk = new AtomicInteger();

    private final File indexFile;

    private final IndexWriteTask flushTask;

    private volatile int diskCapacity;

    private final RegisteredEventListeners cacheEventNotificationService;
    private final boolean diskPersistent;

    /**
     * Constructs an disk persistent factory for the given cache and disk path.
     *
     * @param cache cache that fronts this factory
     * @param diskPath path to store data in
     * @param cacheEventNotificationService the notification service
     */
    public DiskStorageFactory(Ehcache cache, String diskPath, RegisteredEventListeners cacheEventNotificationService) {
        this.file = getDataFile(diskPath, cache);
        this.indexFile = new File(getDataFile().getParentFile(), getIndexFileName(cache));

        diskPersistent = cache.getCacheConfiguration().isDiskPersistent();
        if (!diskPersistent) {
            deleteFile(file);
            deleteFile(indexFile);
        }

        try {
            dataAccess = allocateRandomAccessFiles(file, cache.getCacheConfiguration().getDiskAccessStripes());
        } catch (FileNotFoundException e) {
            throw new CacheException(e);
        }
        this.allocator = new FileAllocationTree(Long.MAX_VALUE, dataAccess[0]);

        diskWriter = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, file.getName());
                t.setDaemon(false);
                return t;
            }
        });
        this.diskQueue = diskWriter.getQueue();
        this.eventService = cache.getCacheEventNotificationService();
        this.queueCapacity = cache.getCacheConfiguration().getDiskSpoolBufferSizeMB() * MEGABYTE;

        diskWriter.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        diskWriter.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        long expiryInterval = cache.getCacheConfiguration().getDiskExpiryThreadIntervalSeconds();
        diskWriter.scheduleWithFixedDelay(new DiskExpiryTask(), expiryInterval, expiryInterval, TimeUnit.SECONDS);



        this.cacheEventNotificationService = cacheEventNotificationService;

        flushTask = new IndexWriteTask(indexFile, cache.getCacheConfiguration().isClearOnFlush());

        if (!getDataFile().exists() || (getDataFile().length() == 0)) {
            LOG.debug("Matching data file missing (or empty) for index file. Deleting index file " + indexFile);
            deleteFile(indexFile);
        } else if (getDataFile().exists() && indexFile.exists()) {
            if (getDataFile().lastModified() > (indexFile.lastModified() + TimeUnit.SECONDS.toMillis(1))) {
                LOG.warn("The index for data file {} is out of date, probably due to an unclean shutdown. "
                        + "Deleting index file {}", getDataFile(), indexFile);
                deleteFile(indexFile);
            }
        }

        diskCapacity = cache.getCacheConfiguration().getMaxElementsOnDisk();
    }

    private static RandomAccessFile[] allocateRandomAccessFiles(File f, int stripes) throws FileNotFoundException {
        int roundedStripes = stripes;
        while ((roundedStripes & (roundedStripes - 1)) != 0) {
            ++roundedStripes;
        }

        RandomAccessFile [] result = new RandomAccessFile[roundedStripes];
        for (int i = 0; i < result.length; ++i) {
            result[i] = new RandomAccessFile(f, "rw");
        }

        return result;
    }

    private RandomAccessFile getDataAccess(Object key) {
        return this.dataAccess[ConcurrencyUtil.selectLock(key, dataAccess.length)];
    }

    /**
     * Return this size in bytes of this factory
     */
    public long getOnDiskSizeInBytes() {
        synchronized (dataAccess[0]) {
            try {
                return dataAccess[0].length();
            } catch (IOException e) {
                LOG.warn("Exception trying to determine store size", e);
                return 0;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void bind(DiskStore store) {
        this.store = store;
        loadIndex();
    }

    /**
     * {@inheritDoc}
     */
    public void free(Lock lock, DiskSubstitute substitute) {
        if (substitute instanceof DiskStorageFactory.DiskMarker) {
            onDisk.decrementAndGet();
            //free done asynchronously under the relevant segment lock...
            DiskFreeTask free = new DiskFreeTask(lock, (DiskMarker) substitute);
            if (lock.tryLock()) {
                try {
                    free.call();
                } finally {
                    lock.unlock();
                }
            } else {
                schedule(free);
            }
        }
    }

    /**
     * Mark this on-disk marker as used (hooks into the file space allocation structure).
     */
    protected void markUsed(DiskMarker marker) {
        allocator.mark(new Region(marker.getPosition(), marker.getPosition() + marker.getSize() - 1));
    }

    /**
     * Shrink this store's data file down to a minimal size for its contents.
     */
    protected void shrinkDataFile() {
        synchronized (dataAccess[0]) {
            try {
                dataAccess[0].setLength(allocator.getFileSize());
            } catch (IOException e) {
                LOG.error("Exception trying to shrink data file to size", e);
            }
        }
    }
    /**
     * Shuts down this disk factory.
     * <p>
     * This shuts down the executor and then waits for its termination, before closing the data file.
     */
    protected void shutdown() throws IOException {
        diskWriter.shutdown();
        for (int i = 0; i < SHUTDOWN_GRACE_PERIOD; i++) {
            try {
                if (diskWriter.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                } else {
                    LOG.info("Waited " + (i + 1) + " seconds for shutdown of [" + file.getName() + "]");
                }
            } catch (InterruptedException e) {
                LOG.warn("Received exception while waiting for shutdown", e);
            }
        }

        for (RandomAccessFile raf : dataAccess) {
            synchronized (raf) {
                raf.close();
            }
        }

        if (!diskPersistent) {
            deleteFile(file);
            deleteFile(indexFile);
        }
    }

    /**
     * Deletes the data file for this factory.
     */
    protected void delete() {
        deleteFile(file);
        allocator.clear();
        if (file.getAbsolutePath().contains(AUTO_DISK_PATH_DIRECTORY_PREFIX)) {
            //try to delete the auto_createtimestamp directory. Will work when the last Disk Store deletes
            //the last files and the directory becomes empty.
            File dataDirectory = file.getParentFile();
            if (dataDirectory != null && dataDirectory.exists()) {
                if (dataDirectory.delete()) {
                    LOG.debug("Deleted directory " + dataDirectory.getName());
                }
            }

        }
    }

    /**
     * Schedule to given task on the disk writer executor service.
     *
     * @param <U> return type of the callable
     * @param call callable to call
     * @return Future representing the return of this call
     */
    protected <U> Future<U> schedule(Callable<U> call) {
        return diskWriter.submit(call);
    }

    /**
     * Read the data at the given marker, and return the associated deserialized Element.
     *
     * @param marker marker to read
     * @return deserialized Element
     * @throws java.io.IOException on read error
     * @throws ClassNotFoundException on deserialization error
     */
    protected Element read(DiskMarker marker) throws IOException, ClassNotFoundException {
        final byte[] buffer = new byte[marker.getSize()];
        RandomAccessFile data = getDataAccess(marker.getKey());
        synchronized (data) {
            // Load the element
            data.seek(marker.getPosition());
            data.readFully(buffer);
        }

        ObjectInputStream objstr = new ObjectInputStream(new ByteArrayInputStream(buffer)) {
            /**
             * Overridden because of:
             * Bug 1324221 ehcache DiskStore has issues when used in Tomcat
             */
            @Override
            protected Class resolveClass(ObjectStreamClass clazz) throws ClassNotFoundException, IOException {
                try {
                    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                    return Class.forName(clazz.getName(), false, classLoader);
                } catch (ClassNotFoundException e) {
                    // Use the default as a fallback because of
                    // bug 1517565 - DiskStore loadElementFromDiskElement
                    return super.resolveClass(clazz);
                }
            }
        };

        try {
            return (Element) objstr.readObject();
        } finally {
            objstr.close();
        }
    }

    /**
     * Write the given element to disk, and return the associated marker.
     *
     * @param element to write
     * @return marker representing the element
     * @throws java.io.IOException on write error
     */
    protected DiskMarker write(Element element) throws IOException {
        Element elementToSerialize = element;
        if (element.getObjectValue() instanceof SoftLock) {
            elementToSerialize = null;
        }

        MemoryEfficientByteArrayOutputStream buffer = serializeElement(elementToSerialize);
        int bufferLength = buffer.size();
        elementSize = bufferLength;
        DiskMarker marker = alloc(element, bufferLength);
        // Write the record
        RandomAccessFile data = getDataAccess(element.getObjectKey());
        synchronized (data) {
            data.seek(marker.getPosition());
            data.write(buffer.toByteArray(), 0, bufferLength);
        }
        return marker;
    }

    private MemoryEfficientByteArrayOutputStream serializeElement(Element element) throws IOException {
        // try two times to Serialize. A ConcurrentModificationException can occur because Java's serialization
        // mechanism is not threadsafe and POJOs are seldom implemented in a threadsafe way.
        // e.g. we are serializing an ArrayList field while another thread somewhere in the application is appending to it.
        // The best we can do is try again and then give up.
        ConcurrentModificationException exception = null;
        for (int retryCount = 0; retryCount < 2; retryCount++) {
            try {
                return MemoryEfficientByteArrayOutputStream.serialize(element);
            } catch (ConcurrentModificationException e) {
                exception = e;
                try {
                    // wait for the other thread(s) to finish
                    MILLISECONDS.sleep(SERIALIZATION_CONCURRENCY_DELAY);
                } catch (InterruptedException e1) {
                    //no-op
                }
            }
        }
        throw exception;
    }

    private DiskMarker alloc(Element element, int size) throws IOException {
        //check for a matching chunk
        Region r = allocator.alloc(size);
        return createMarker(r.start(), size, element);
    }

    /**
     * Free the given marker to be used by a subsequent write.
     *
     * @param marker marker to be free'd
     */
    protected void free(DiskMarker marker) {
        allocator.free(new Region(marker.getPosition(), marker.getPosition() + marker.getSize() - 1));
    }

    /**
     * Return {@code true} if the disk write queue is full.
     */
    public boolean bufferFull() {
        return (diskQueue.size() * elementSize) > queueCapacity;
    }

    /**
     * Return a reference to the data file backing this factory.
     */
    public File getDataFile() {
        return file;
    }

    /**
     * DiskWriteTasks are used to serialize elements
     * to disk and fault in the resultant DiskMarker
     * instance.
     */
    abstract class DiskWriteTask implements Callable<DiskMarker> {

        private final Placeholder placeholder;

        /**
         * Create a disk-write task for the given placeholder.
         */
        DiskWriteTask(Placeholder p) {
            this.placeholder = p;
        }

        /**
         * Return the placeholder that this task will write.
         */
        Placeholder getPlaceholder() {
            return placeholder;
        }

        /**
         * {@inheritDoc}
         */
        public DiskMarker call() {
            try {
                DiskMarker marker = write(placeholder.getElement());
                if (marker != null && store.fault(placeholder.getKey(), placeholder, marker)) {
                    return marker;
                } else {
                    return null;
                }
            } catch (IOException e) {
                LOG.error("Disk Write of " + placeholder.getKey() + " failed (it will be evicted instead): ", e);
                store.evict(placeholder.getKey(), placeholder);
                return null;
            }
        }
    }

    /**
     * Disk free tasks are used to asynchronously free DiskMarker instances under the correct
     * exclusive write lock.  This ensure markers are not free'd until no more readers can be
     * holding references to them.
     */
    private final class DiskFreeTask implements Callable<Void> {
        private final Lock lock;
        private final DiskMarker marker;

        private DiskFreeTask(Lock lock, DiskMarker marker) {
            this.lock = lock;
            this.marker = marker;
        }

        /**
         * {@inheritDoc}
         */
        public Void call() {
            lock.lock();
            try {
                DiskStorageFactory.this.free(marker);
            } finally {
                lock.unlock();
            }
            return null;
        }
    }

    /**
     * Abstract superclass for all disk substitutes.
     */
    public abstract static class DiskSubstitute {

        @IgnoreSizeOf
        private transient volatile DiskStorageFactory factory;

        /**
         * Create a disk subsitute bound to no factory.  This constructor is used during
         * de-serialization.
         */
        public DiskSubstitute() {
            this.factory = null;
        }

        /**
         * Create a disk subsitute bound to the given factory.
         */
        DiskSubstitute(DiskStorageFactory factory) {
            this.factory = factory;
        }

        /**
         * Return the key to which this marker is (or should be) mapped.
         */
        abstract Object getKey();

        /**
         * Return the total number of hits on this marker
         */
        abstract long getHitCount();

        /**
         * Return the time at which this marker expires.
         */
        abstract long getExpirationTime();

        abstract void installed();

        /**
         * {@inheritDoc}
         */
        public final DiskStorageFactory getFactory() {
            return factory;
        }

        /**
         * Bind this marker to a given factory.
         * <p>
         * Used during deserialization of markers to associate them with the deserializing factory.
         */
        void bindFactory(DiskStorageFactory factory) {
            this.factory = factory;
        }
    }

    /**
     * Placeholder instances are put in place to prevent
     * duplicate write requests while Elements are being
     * written to disk.
     */
    private final class Placeholder extends DiskSubstitute {
        @IgnoreSizeOf
        private final Object key;
        private final Element element;

        /**
         * Create a Placeholder wrapping the given element and key.
         */
        Placeholder(Element element) {
            super(DiskStorageFactory.this);
            this.key = element.getObjectKey();
            this.element = element;
        }


        /**
         * {@inheritDoc}
         */
        public void installed() {
            DiskStorageFactory.this.schedule(new PersistentDiskWriteTask(this));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        Object getKey() {
            return key;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        long getHitCount() {
            return getElement().getHitCount();
        }

        @Override
        long getExpirationTime() {
            return getElement().getExpirationTime();
        }

        /**
         * Return the element that this Placeholder is wrapping.
         */
        Element getElement() {
            return element;
        }
    }

    /**
     * DiskMarker instances point to the location of their
     * associated serialized Element instance.
     */
    public static class DiskMarker extends DiskSubstitute implements Serializable {

        @IgnoreSizeOf
        private final Object key;

        private final long position;
        private final int size;

        private final long hitCount;

        private volatile long expiry;

        /**
         * Create a new marker tied to the given factory instance.
         *
         * @param factory factory responsible for this marker
         * @param position position on disk where the element will be stored
         * @param size size of the serialized element
         * @param element element being stored
         */
        DiskMarker(DiskStorageFactory factory, long position, int size, Element element) {
            super(factory);
            this.position = position;
            this.size = size;

            this.key = element.getObjectKey();
            this.hitCount = element.getHitCount();
            this.expiry = element.getExpirationTime();
        }

        /**
         * Create a new marker tied to the given factory instance.
         *
         * @param factory factory responsible for this marker
         * @param position position on disk where the element will be stored
         * @param size size of the serialized element
         * @param key key to which this element is mapped
         * @param hits hit count for this element
         */
        DiskMarker(DiskStorageFactory factory, long position, int size, Object key, long hits) {
            super(factory);
            this.position = position;
            this.size = size;

            this.key = key;
            this.hitCount = hits;
        }

        /**
         * Key to which this Element is mapped.
         *
         * @return key for this Element
         */
        @Override
        Object getKey() {
            return key;
        }

        /**
         * Number of hits on this Element.
         */
        @Override
        long getHitCount() {
            return hitCount;
        }

        /**
         * Disk offset at which this element is stored.
         *
         * @return disk offset
         */
        private long getPosition() {
            return position;
        }

        /**
         * Returns the size of the currently occupying element.
         *
         * @return size of the stored element
         */
        public int getSize() {
            return size;
        }

        /**
         * {@inheritDoc}
         * <p>
         * A No-Op
         */
        public void installed() {
            //no-op
        }

        @Override
        long getExpirationTime() {
            return expiry;
        }
    }


    /**
     * Remove elements created by this factory if they have expired.
     */
    public void expireElements() {
        new DiskExpiryTask().run();
    }

    /**
     * Causes removal of all expired elements (and fires the relevant events).
     */
    private final class DiskExpiryTask implements Runnable {

        /**
         * {@inheritDoc}
         */
        public void run() {
            long now = System.currentTimeMillis();
            for (Object key : store.keySet()) {
                Object value = store.unretrievedGet(key);
                if (created(value) && value instanceof DiskStorageFactory.DiskMarker) {
                    checkExpiry((DiskMarker) value, now);
                }
            }
        }

        private void checkExpiry(DiskMarker marker, long now) {
            if (marker.getExpirationTime() < now) {
                if (eventService.hasCacheEventListeners()) {
                    try {
                        Element element = read(marker);
                        if (store.evict(marker.getKey(), marker)) {
                            eventService.notifyElementExpiry(element, false);
                        }
                    } catch (Exception e) {
                        return;
                    }
                } else {
                    store.evict(marker.getKey(), marker);
                }
            }
        }
    }

    /**
     * Attempt to delete the corresponding file and log an error on failure.
     */
    protected static void deleteFile(File f) {
        if (!f.delete()) {
            LOG.debug("Failed to delete file {}", f.getName());
        }
    }


    private static File getDataFile(String diskPath, Ehcache cache) {
        if (diskPath == null) {
            throw new CacheException(cache.getName() + " Cache: Could not create disk store. "
                    + "This CacheManager configuration does not allow creation of DiskStores. "
                    + "If you wish to create DiskStores, please configure a diskStore path.");
        }

        final File diskDir = new File(diskPath);
        // Make sure the cache directory exists
        if (diskDir.exists() && !diskDir.isDirectory()) {
            throw new CacheException("Store directory \"" + diskDir.getAbsolutePath() + "\" exists and is not a directory.");
        }
        if (!diskDir.exists() && !diskDir.mkdirs()) {
            throw new CacheException("Could not create cache directory \"" + diskDir.getAbsolutePath() + "\".");
        }

        File data = new File(diskDir, getDataFileName(cache));

        //if diskpath contains auto generated string
        if (diskPath.indexOf(AUTO_DISK_PATH_DIRECTORY_PREFIX) != -1) {
            LOG.warn("Data in persistent disk stores is ignored for stores from automatically created directories"
                    + " (they start with " + AUTO_DISK_PATH_DIRECTORY_PREFIX + ").\n"
                    + "Remove diskPersistent or resolve the conflicting disk paths in cache configuration.\n"
                    + "Deleting data file " + data.getAbsolutePath());
            deleteFile(data);
        }

        return data;
    }

    private static final String getDataFileName(Ehcache cache) {
        String safeName = cache.getName().replace('/', '_');
        return safeName + ".data";
    }

    private static final String getIndexFileName(Ehcache cache) {
        String safeName = cache.getName().replace('/', '_');
        return safeName + ".index";
    }

    /**
     * {@inheritDoc}
     */
    public DiskSubstitute create(Element element) throws IllegalArgumentException {
        return new Placeholder(element);
    }

    /**
     * {@inheritDoc}
     */
    public Element retrieve(DiskSubstitute object) {
        if (object instanceof DiskMarker) {
            try {
                DiskMarker marker = (DiskMarker) object;
                return read(marker);
            } catch (IOException e) {
                throw new CacheException(e);
            } catch (ClassNotFoundException e) {
                throw new CacheException(e);
            }
        } else if (object instanceof Placeholder) {
            return ((Placeholder) object).getElement();
        } else {
            return null;
        }
    }

    public Element retrieve(DiskSubstitute object, RetrievalStatistic statistic) {
        if (object instanceof DiskMarker) {
            try {
                DiskMarker marker = (DiskMarker) object;
                statistic.diskHit();
                return read(marker);
            } catch (IOException e) {
                throw new CacheException(e);
            } catch (ClassNotFoundException e) {
                throw new CacheException(e);
            }
        } else if (object instanceof DiskStorageFactory.Placeholder) {
            statistic.diskHit();
            return ((Placeholder) object).getElement();
        } else {
            statistic.miss();
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean created(Object object) {
        if (object instanceof DiskSubstitute) {
            return ((DiskSubstitute) object).getFactory() == this;
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void unbind() {
        try {
            flushTask.call();
            shutdown();
            if (getDataFile().getAbsolutePath().contains(AUTO_DISK_PATH_DIRECTORY_PREFIX)) {
                deleteFile(indexFile);
                delete();
            }
        } catch (IOException e) {
            LOG.error("Could not shut down disk cache. Initial cause was " + e.getMessage(), e);
        }
    }

    /**
     * Schedule a flush (index write) for this factory.
     */
    public Future<Void> flush() {
        return schedule(flushTask);
    }

    protected DiskMarker createMarker(long position, int size, Element element) {
        return new DiskMarker(this, position, size, element);
    }

    public int evict(int count) {
        int evicted = 0;
        for (int i = 0; i < count; i++) {
            DiskSubstitute target = this.getDiskEvictionTarget(null, count);
            if (target != null) {
                Element evictedElement = store.evictElement(target.getKey(), null);
                if (cacheEventNotificationService!= null) {
                    cacheEventNotificationService.notifyElementEvicted(evictedElement, false);
                }
                evicted++;
            }
        }
        return evicted;
    }

    /**
     * Filters for on-disk elements created by this factory
     */
    private class OnDiskFilter implements ElementSubstituteFilter {

        /**
         * {@inheritDoc}
         */
        public boolean allows(Object object) {
            if (!created(object)) {
                return false;
            }

            return object instanceof DiskMarker;
        }
    }

    /**
     * Return {@code true} if the given element is on disk
     */
    public boolean isOnDisk(Object object) {
        return onDiskFilter.allows(object);
    }


    /**
     * Return the number of on-disk elements
     */
    public int getOnDiskSize() {
        return onDisk.get();
    }

    /**
     * Set the maximum on-disk capacity for this factory.
     */
    public void setOnDiskCapacity(int capacity) {
        diskCapacity = capacity;
    }

    private void onDiskEvict(int size, Object keyHint) {
        if (diskCapacity > 0) {
            int overflow = size - diskCapacity;
            for (int i = 0; i < Math.min(MAX_EVICT, overflow); i++) {
                DiskSubstitute target = getDiskEvictionTarget(keyHint, size);
                if (target == null) {
                    continue;
                } else {
                    if (store.evict(target.getKey(), target) && (onDisk.get() <= diskCapacity)) {
                        break;
                    }
                }
            }
        }
    }

    private DiskSubstitute getDiskEvictionTarget(Object keyHint, int size) {
        List<DiskMarker> sample = store.getRandomSample(onDiskFilter, Math.min(SAMPLE_SIZE, size), keyHint);
        DiskMarker target = null;
        for (DiskMarker substitute : sample) {
            if ((target == null) || (substitute.getHitCount() < target.getHitCount())) {
                target = substitute;
            }
        }
        return target;
    }

    /**
     * Disk write task implementation for disk persistent stores.
     */
    private final class PersistentDiskWriteTask extends DiskWriteTask {

        /**
         * Create a disk persistent disk-write task for this placeholder.
         */
        PersistentDiskWriteTask(Placeholder p) {
            super(p);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DiskMarker call() {
            DiskMarker result = super.call();
            //don't want to increment on exception throw
            int disk = onDisk.incrementAndGet();
            onDiskEvict(disk, getPlaceholder().getKey());
            return result;
        }
    }

    /**
     * Task that writes the index file for this factory.
     */
    class IndexWriteTask implements Callable<Void> {

        private final File index;
        private final boolean clearOnFlush;

        /**
         * Create a disk flush task that writes to the given file.
         */
        IndexWriteTask(File index, boolean clear) {
            this.index = index;
            this.clearOnFlush = clear;
        }

        /**
         * {@inheritDoc}
         */
        public synchronized Void call() throws IOException {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(index));
            try {
                for (Object key : store.keySet()) {
                    Object o = store.unretrievedGet(key);
                    if (o instanceof Placeholder) {
                        o = new PersistentDiskWriteTask((Placeholder) o).call();
                        if (o == null) {
                            o = store.unretrievedGet(key);
                        }
                    }

                    if (o instanceof DiskMarker) {
                        DiskMarker marker = (DiskMarker) o;
                        oos.writeObject(key);
                        oos.writeObject(marker);
                    }
                }
            } finally {
                oos.close();
            }
            return null;
        }

    }

    private void loadIndex() {
        if (!indexFile.exists()) {
            return;
        }

        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(indexFile));
            try {
                Object key = ois.readObject();
                Object value = ois.readObject();

                DiskMarker marker = (DiskMarker) value;
                while (true) {
                    marker.bindFactory(this);
                    if (store.putRawIfAbsent(key, marker)) {
                        onDisk.incrementAndGet();
                    } else {
                        throw new AssertionError();
                    }
                    markUsed(marker);
                    key = ois.readObject();
                    marker = (DiskMarker) ois.readObject();
                }
            } finally {
                ois.close();
            }
        } catch (EOFException e) {
            return;
        } catch (Exception e) {
            LOG.warn("Index file {} is corrupt, deleting and ignoring it : {}", indexFile, e);
            e.printStackTrace();
            store.removeAll();
            deleteFile(indexFile);
        } finally {
            shrinkDataFile();
        }
    }

    /**
     * Return the index file for this store.
     */
    public File getIndexFile() {
        return indexFile;
    }
}
