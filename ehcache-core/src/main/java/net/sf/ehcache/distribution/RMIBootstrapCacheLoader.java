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

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.bootstrap.BootstrapCacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

/**
 * Loads Elements from a random Cache Peer
 *
 * @author Greg Luck
 * @version $Id$
 */
public class RMIBootstrapCacheLoader implements BootstrapCacheLoader, Cloneable {

    private static final int ONE_SECOND = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(RMIBootstrapCacheLoader.class);

    private final SecureRandom secureRandom = new SecureRandom();

    /**
     * Whether to load asynchronously
     */
    protected boolean asynchronous;

    /**
     * The maximum serialized size of the elements to request from a remote cache peer during bootstrap.
     */
    protected int maximumChunkSizeBytes;

    /**
     * Creates a boostrap cache loader that will work with RMI based distribution
     *
     * @param asynchronous Whether to load asynchronously
     */
    public RMIBootstrapCacheLoader(boolean asynchronous, int maximumChunkSize) {
        this.asynchronous = asynchronous;
        this.maximumChunkSizeBytes = maximumChunkSize;
    }


    /**
     * Bootstraps the cache from a random CachePeer. Requests are done in chunks estimated at 5MB Serializable
     * size. This balances memory use on each end and network performance.
     *
     * @throws RemoteCacheException if anything goes wrong with the remote call
     */
    public void load(Ehcache cache) throws RemoteCacheException {
        if (asynchronous) {
            Thread bootstrapThread = new Thread(null, () -> doLoad(cache), "Bootstrap Thread for cache " + cache.getName());
            bootstrapThread.setDaemon(true);
            bootstrapThread.start();
        } else {
            doLoad(cache);
        }
    }

    /**
     * @return true if this bootstrap loader is asynchronous
     */
    public boolean isAsynchronous() {
        return asynchronous;
    }

    /**
     * Bootstraps the cache from a random CachePeer. Requests are done in chunks estimated at 5MB Serializable
     * size. This balances memory use on each end and network performance.
     * <p>
     * Bootstrapping requires the establishment of a cluster. This can be instantaneous for manually configued
     * clusters or may take a number of seconds for multicast ones. This method waits up to 11 seconds for a cluster
     * to form.
     *
     * @throws RemoteCacheException if anything goes wrong with the remote call
     */
    public void doLoad(Ehcache cache) {

        Optional<CachePeer> cachePeerOptional = getRandomCachePeer(cache);
        if (!cachePeerOptional.isPresent()) {
            LOG.debug("Empty list of cache peers for cache " + cache.getName() + ". No cache peer to bootstrap from.");
            return;
        }

        try {
            CachePeer cachePeer = cachePeerOptional.get();
            String rmiUrl = cachePeer.getUrl();

            LOG.debug("start bootstrapping cache {} from {}", cache.getName(), rmiUrl);
            List<Serializable> keys = cachePeer.getKeys();

            // estimate element size
            Optional<Element> sampleElement = getSampleElement(cachePeer, keys);
            if (!sampleElement.isPresent()) {
                LOG.debug("All cache peer elements on {} were either null or empty. Nothing to bootstrap from.", rmiUrl);

                return;
            }

            AtomicInteger counter = new AtomicInteger();
            long chunkSize = maximumChunkSizeBytes / sampleElement.get().getSerializedSize();
            int numberOfChunks = (int) Math.ceil((double) keys.size() / chunkSize);
            getChunks(keys, chunkSize).forEach(chunk -> {
                try {
                    LOG.debug("starting with chunk: {} of {}", counter.incrementAndGet(), numberOfChunks);
                    cachePeer.getElements(chunk)
                            .stream()
                            .filter(Objects::nonNull)
                            .forEach(element -> cache.put(element, true));
                } catch (RemoteException e) {
                    throw new RemoteCacheException("could not fetch elements from " + rmiUrl, e);
                }
            });

            LOG.debug("Bootstrap of cache {} from peer {} finished. Got {} keys requested.", cache.getName(), rmiUrl, keys.size());
        } catch (Exception e) {
            throw new RemoteCacheException("Error bootstrapping from remote peer.", e);
        }
    }

    private Collection<List<Serializable>> getChunks(List<Serializable> keys, long chunkSize) {
        AtomicLong counter = new AtomicLong();
        return keys
                .stream()
                .collect(Collectors.groupingBy(key -> counter.getAndIncrement() / chunkSize))
                .values();
    }

    Optional<Element> getSampleElement(CachePeer cachePeer, List<Serializable> keys) throws RemoteException {
        for (Serializable key : keys) {
            Element element = cachePeer.getQuiet(key);
            if (element != null && element.getSerializedSize() != 0) {
                return Optional.of(element);
            }
        }

        return Optional.empty();
    }

    Optional<CachePeer> getRandomCachePeer(Ehcache cache) {
        List<CachePeer> cachePeers = acquireCachePeers(cache);
        if (cachePeers.isEmpty()) {
            return Optional.empty();
        }
        int randomIndex = secureRandom.nextInt(cachePeers.size());

        return Optional.ofNullable(cachePeers.get(randomIndex));
    }

    /**
     * Acquires the cache peers for this cache.
     */
    List<CachePeer> acquireCachePeers(Ehcache cache) {

        CacheManagerPeerProvider cacheManagerPeerProvider = getPeerProvider(cache);
        long timeForClusterToForm = cacheManagerPeerProvider.getTimeForClusterToForm();
        LOG.debug("Attempting to acquire cache peers for cache {} to bootstrap from. Will wait up to {}ms for cache to join cluster.", cache.getName(), timeForClusterToForm);
        try {
            for (int i = 0; i <= timeForClusterToForm; i += ONE_SECOND) {
                List<CachePeer> cachePeers = cacheManagerPeerProvider.listRemoteCachePeers(cache);
                if (!cachePeers.isEmpty()) {
                    return cachePeers;
                }
                sleep(ONE_SECOND);
            }
        } catch (InterruptedException e) {
            LOG.debug("doLoad for " + cache.getName() + " interrupted.");
            return Collections.emptyList();
        }

        LOG.info("no remote peer joined within {}ms.", timeForClusterToForm);

        return Collections.emptyList();
    }

    CacheManagerPeerProvider getPeerProvider(Ehcache cache) {
        CacheManagerPeerProvider peerProvider = cache.getCacheManager().getCacheManagerPeerProvider("RMI");
        if (peerProvider == null) {
            throw new IllegalStateException("no RMI peer provider configured.");
        }
        return peerProvider;
    }

    /**
     * Gets the maximum chunk size
     */
    public int getMaximumChunkSizeBytes() {
        return maximumChunkSizeBytes;
    }

    /**
     * Clones this loader
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        //checkstyle
        return new RMIBootstrapCacheLoader(asynchronous, maximumChunkSizeBytes);
    }

}
