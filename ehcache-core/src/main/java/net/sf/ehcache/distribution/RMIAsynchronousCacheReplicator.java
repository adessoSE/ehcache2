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

import net.sf.ehcache.distribution.RmiEventMessage.RmiEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.sleep;

/**
 * Listens to {@link net.sf.ehcache.CacheManager} and {@link net.sf.ehcache.Cache} events and propagates those to
 * {@link CachePeer} peers of the Cache asynchronously.
 * <p>
 * Updates are guaranteed to be replicated in the order in which they are received.
 * <p>
 * While much faster in operation than {@link RMISynchronousCacheReplicator}, it does suffer from a number
 * of problems. Elements, which may be being spooled to DiskStore may stay around in memory because references
 * are being held to them from {@link EventMessage}s which are queued up. The replication thread runs once
 * per second, limiting the build up. However a lot of elements can be put into a cache in that time. We do not want
 * to get an {@link OutOfMemoryError} using distribution in circumstances when it would not happen if we were
 * just using the DiskStore.
 * <p>
 * Accordingly, the Element values in {@link EventMessage}s are held by {@link java.lang.ref.SoftReference} in the queue,
 * so that they can be discarded if required by the GC to avoid an {@link OutOfMemoryError}. A log message
 * will be issued on each flush of the queue if there were any forced discards. One problem with GC collection
 * of SoftReferences is that the VM (JDK1.5 anyway) will do that rather than grow the heap size to the maximum.
 * The workaround is to either set minimum heap size to the maximum heap size to force heap allocation at start
 * up, or put up with a few lost messages while the heap grows.
 *
 * @author Greg Luck
 */
public class RMIAsynchronousCacheReplicator extends AbstractRMICacheReplicator {

    private static final Logger LOG = LoggerFactory.getLogger(RMIAsynchronousCacheReplicator.class);
    private static final String REPLICATION_THREAD_NAME = "Replication Thread";

    /**
     * A queue of changes.
     */
    protected final Queue<Object> replicationQueue = new ConcurrentLinkedQueue<>();

    /**
     * A thread which handles replication, so that replication can take place asynchronously and not hold up the cache
     */
    private final Thread replicationThread = new Thread(null, this::replicate, REPLICATION_THREAD_NAME);

    /**
     * The amount of time the replication thread sleeps after it detects the replicationQueue is empty
     * before checking again.
     */
    private final int replicationInterval;

    /**
     * The maximum number of Element replication in single RMI message.
     */
    private final int maximumBatchSize;

    /**
     * Constructor for internal and subclass use
     */
    public RMIAsynchronousCacheReplicator(
            boolean replicatePuts,
            boolean replicatePutsViaCopy,
            boolean replicateUpdates,
            boolean replicateUpdatesViaCopy,
            boolean replicateRemovals,
            int replicationInterval,
            int maximumBatchSize) {
        super(replicatePuts, replicatePutsViaCopy, replicateUpdates, replicateUpdatesViaCopy, replicateRemovals);

        this.replicationInterval = replicationInterval;
        this.maximumBatchSize = maximumBatchSize;
        this.replicationThread.setDaemon(true);

        this.replicationThread.start();
    }


    /**
     * Adds a message to the queue.
     * <p>
     * This method checks the state of the replication thread and warns
     * if it has stopped and then discards the message.
     *
     * @param eventMessage the message to process.
     */
    protected void processEvent(RmiEventMessage eventMessage) {
        if (!replicationThread.isAlive()) {
            LOG.error("CacheEventMessages cannot be added to the replication queue because the replication thread has died.");
            return;
        }

        if (eventMessage.getType() == RmiEventType.PUT) {
            replicationQueue.add(new SoftReference<>(eventMessage));
        } else {
            replicationQueue.add(eventMessage);
        }
    }

    /**
     * Gets called once per {@link #replicationInterval}.
     * <p>
     * Sends accumulated messages in bulk to each peer. i.e. if ther are 100 messages and 1 peer,
     * 1 RMI invocation results, not 100. Also, if a peer is unavailable this is discovered in only 1 try.
     * <p>
     * Makes a copy of the queue so as not to hold up the enqueue operations.
     * <p>
     * Any exceptions are caught so that the replication thread does not die, and because errors are expected,
     * due to peers becoming unavailable.
     * <p>
     * This method issues warnings for problems that can be fixed with configuration changes.
     */
    void writeReplicationQueue() {
        List<RmiEventMessage> eventMessages = extractEventMessages(replicationQueue, maximumBatchSize);

        Optional<RmiEventMessage> firstMessage = eventMessages.stream().findFirst();
        if (!firstMessage.isPresent()) {
            LOG.debug("empty eventMessages list");
            return;
        }

        for (CachePeer cachePeer : listRemoteCachePeers(firstMessage.get().getEhcache())) {
            try {
                LOG.debug("sending {} messages to {}", eventMessages.size(), cachePeer);
                cachePeer.send(eventMessages);
            } catch (RemoteException e) {
                LOG.error("Unable to send message to remote peer.", e);
            } finally {
                LOG.debug("send completed to {}", cachePeer);
            }
        }
    }

    void flushReplicationQueue() {
        while (!replicationQueue.isEmpty()) {
            writeReplicationQueue();
        }
    }

    /**
     * Extracts CacheEventMessages and attempts to get a hard reference to the underlying EventMessage
     * <p>
     * If an EventMessage has been invalidated due to SoftReference collection of the Element, it is not
     * propagated. This only affects puts and updates via copy.
     *
     * @param queue       the list of queued entries
     * @param maxListSize the maximum size of the list to be returned
     * @return a list of event messages which are able to be replicate
     */
    @SuppressWarnings("unchecked")
    List<RmiEventMessage> extractEventMessages(Queue<Object> queue, int maxListSize) {
        List<RmiEventMessage> list = new ArrayList<>();

        int droppedMessages = 0;

        while (list.size() < maxListSize) {

            Object polled = queue.poll();
            if (polled == null) {
                break;
            }

            if (polled instanceof RmiEventMessage) {
                list.add((RmiEventMessage) polled);
                continue;
            }

            RmiEventMessage message = ((SoftReference<RmiEventMessage>) polled).get();
            if (message != null) {
                list.add(message);
            } else {
                droppedMessages++;
            }
        }

        if (droppedMessages > 0) {
            LOG.warn("{} messages were discarded on replicate due to reclamation of SoftReferences by the VM. " +
                            "Consider increasing the maximum heap size and/or setting the starting heap size to a higher value.",
                    droppedMessages);
        }

        return list;
    }

    void replicate() {
        while (true) {
            // Wait for elements in the replicationQueue
            while (alive() && replicationQueue.isEmpty()) {
                try {
                    sleep(replicationInterval);
                } catch (InterruptedException e) {
                    LOG.debug("{} interrupted.", REPLICATION_THREAD_NAME);
                    return;
                }
            }
            if (notAlive()) {
                return;
            }
            try {
                writeReplicationQueue();
            } catch (Throwable e) {
                LOG.error("Exception on flushing of replication queue. Continuing...", e);
            }
        }
    }

    /**
     * Give the replicator a chance to flush the replication queue, then cleanup and free resources when no longer needed
     */
    @Override
    public void dispose() {
        super.dispose();
        flushReplicationQueue();
    }

    /**
     * Creates a clone of this listener. This method will only be called by ehcache before a cache is initialized.
     * <p>
     * This may not be possible for listeners after they have been initialized. Implementations should throw
     * CloneNotSupportedException if they do not support clone.
     *
     * @return a clone
     * @throws CloneNotSupportedException if the listener could not be cloned.
     */
    public Object clone() throws CloneNotSupportedException {
        // shut up checkstyle
        super.clone();
        return new RMIAsynchronousCacheReplicator(
                replicatePuts,
                replicatePutsViaCopy,
                replicateUpdates,
                replicateUpdatesViaCopy,
                replicateRemovals,
                replicationInterval,
                maximumBatchSize);
    }

}
