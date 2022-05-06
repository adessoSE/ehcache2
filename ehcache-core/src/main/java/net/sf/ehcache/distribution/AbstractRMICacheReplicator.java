/**
 * Copyright Terracotta, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <a href="http://www.apache.org/licenses/LICENSE-2.0">http://www.apache.org/licenses/LICENSE-2.0</a>
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.sf.ehcache.distribution;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This replicator is the base for {@link RMIAsynchronousCacheReplicator} and {@link RMISynchronousCacheReplicator}
 *
 * @author Joerg Schoemer
 */
abstract class AbstractRMICacheReplicator implements CacheReplicator {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRMICacheReplicator.class);

    /**
     * Whether to replicate puts.
     */
    final boolean replicatePuts;

    /**
     * Whether a put should replicated by copy or by invalidation, (a remove).
     * <p>
     * By copy is best when the entry is expensive to produce. By invalidation is best when
     * we are really trying to force other caches to sync back to a canonical source like a database.
     * An example of a latter usage would be a read/write cache being used in Hibernate.
     * <p>
     * This setting only has effect if <code>#replicateUpdates</code> is true.
     */
    final boolean replicatePutsViaCopy;

    /**
     * Whether to replicate updates.
     */
    final boolean replicateUpdates;

    /**
     * Whether an update (a put) should be by copy or by invalidation, (a remove).
     * <p>
     * By copy is best when the entry is expensive to produce. By invalidation is best when
     * we are really trying to force other caches to sync back to a canonical source like a database.
     * An example of a latter usage would be a read/write cache being used in Hibernate.
     * <p>
     * This setting only has effect if <code>#replicateUpdates</code> is true.
     */
    final boolean replicateUpdatesViaCopy;

    /**
     * Whether to replicate removes
     */
    final boolean replicateRemovals;

    /**
     * The status of the replicator. Only replicates when <code>STATUS_ALIVE</code>
     */
    private Status status = Status.STATUS_ALIVE;

    public AbstractRMICacheReplicator(boolean replicatePuts, boolean replicatePutsViaCopy, boolean replicateUpdates, boolean replicateUpdatesViaCopy, boolean replicateRemovals) {
        this.replicatePuts = replicatePuts;
        this.replicatePutsViaCopy = replicatePutsViaCopy;
        this.replicateUpdates = replicateUpdates;
        this.replicateUpdatesViaCopy = replicateUpdatesViaCopy;
        this.replicateRemovals = replicateRemovals;
    }

    protected List<CachePeer> listRemoteCachePeers(Ehcache cache) {
        return cache.getCacheManager().getCacheManagerPeerProvider("RMI").listRemoteCachePeers(cache);
    }

    /**
     * Asserts that the replicator is active.
     *
     * @return true if the status is not STATUS_ALIVE
     */
    public boolean notAlive() {
        return !alive();
    }

    /**
     * Checks that the replicator is is <code>STATUS_ALIVE</code>.
     */
    public boolean alive() {
        return status.equals(Status.STATUS_ALIVE);
    }

    /**
     * @return whether update is through copy or invalidate
     */
    public boolean isReplicateUpdatesViaCopy() {
        return replicateUpdatesViaCopy;
    }

    /**
     * process a single {@link RmiEventMessage}
     */
    protected abstract void processEvent(RmiEventMessage eventMessage);

    void processEvent0(RmiEventMessage eventMessage) {
        if (!eventMessage.isSerializable()) {
            LOG.warn("message='{}' is not Serializable and can't be replicated", eventMessage);
            return;
        }

        processEvent(eventMessage);
    }

    /**
     * Called immediately after an attempt to remove an element. The remove method will block until
     * this method returns.
     * <p>
     * This notification is received regardless of whether the cache had an element matching
     * the removal key or not. If an element was removed, the element is passed to this method,
     * otherwise a synthetic element, with only the key set is passed in.
     * <p>
     *
     * @param cache   the cache emitting the notification
     * @param element the element just deleted, or a synthetic element with just the key set if
     *                no element was removed.param element just deleted
     */
    @SuppressWarnings("deprecation")
    @Override
    public void notifyElementRemoved(Ehcache cache, Element element) throws CacheException {
        if (notAlive() || !replicateRemovals) {
            return;
        }

        processEvent0(new RmiEventMessage(cache, element.getKey()));
    }

    /**
     * Called immediately after an element has been put into the cache. The {@link net.sf.ehcache.Cache#put(net.sf.ehcache.Element)} method
     * will block until this method returns.
     * <p>
     * Implementers may wish to have access to the Element's fields, including value, so the element is provided.
     * Implementers should be careful not to modify the element. The effect of any modifications is undefined.
     *
     * @param cache   the cache emitting the notification
     * @param element the element which was just put into the cache.
     */
    @SuppressWarnings("deprecation")
    @Override
    public void notifyElementPut(Ehcache cache, Element element) throws CacheException {
        if (notAlive() || !replicatePuts) {
            return;
        }

        if (replicatePutsViaCopy) {
            processEvent0(new RmiEventMessage(cache, element));
        } else {
            processEvent0(new RmiEventMessage(cache, element.getKey()));
        }
    }

    /**
     * Called immediately after an element has been put into the cache and the element already
     * existed in the cache. This is thus an update.
     * <p>
     * The {@link net.sf.ehcache.Cache#put(net.sf.ehcache.Element)} method
     * will block until this method returns.
     * <p>
     * Implementers may wish to have access to the Element's fields, including value, so the element is provided.
     * Implementers should be careful not to modify the element. The effect of any modifications is undefined.
     *
     * @param cache   the cache emitting the notification
     * @param element the element which was just put into the cache.
     */
    @SuppressWarnings("deprecation")
    @Override
    public void notifyElementUpdated(Ehcache cache, Element element) throws CacheException {
        if (notAlive() || !replicateUpdates) {
            return;
        }

        if (replicateUpdatesViaCopy) {
            processEvent0(new RmiEventMessage(cache, element));
        } else {
            processEvent0(new RmiEventMessage(cache, element.getKey()));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation does not propagate expiries. It does not need to do anything because the element will
     * expire in the remote cache at the same time. If the remote peer is not configured the same way they should
     * not be in an cache cluster.
     */
    @Override
    public void notifyElementExpired(Ehcache cache, Element element) {
        // not needed
    }

    /**
     * Called immediately after an element is evicted from the cache. Evicted in this sense
     * means evicted from one store and not moved to another, so that it exists nowhere in the
     * local cache.
     * <p>
     * In a sense the Element has been <i>removed</i> from the cache, but it is different,
     * thus the separate notification.
     * <p>
     * This replicator does not propagate these events
     *
     * @param cache   the cache emitting the notification
     * @param element the element that has just been evicted
     */
    @Override
    public void notifyElementEvicted(Ehcache cache, Element element) {
        // not needed
    }

    /**
     * Called during {@link net.sf.ehcache.Ehcache#removeAll()} to indicate that the all
     * elements have been removed from the cache in a bulk operation. The usual
     * {@link #notifyElementRemoved(net.sf.ehcache.Ehcache, net.sf.ehcache.Element)}
     * is not called.
     * <p>
     * This notification exists because clearing a cache is a special case. It is often
     * not practical to serially process notifications where potentially millions of elements
     * have been bulk deleted.
     *
     * @param cache the cache emitting the notification
     */
    @Override
    public void notifyRemoveAll(Ehcache cache) {
        if (notAlive() || !replicateRemovals) {
            return;
        }

        processEvent0(new RmiEventMessage(cache));
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * Give the replicator a chance to flush the replication queue, then cleanup and free resources when no longer needed
     */
    public void dispose() {
        this.status = Status.STATUS_SHUTDOWN;
    }
}
