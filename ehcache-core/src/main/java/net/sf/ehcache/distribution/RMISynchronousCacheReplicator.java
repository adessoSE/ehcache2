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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;

/**
 * Listens to {@link net.sf.ehcache.CacheManager} and {@link net.sf.ehcache.Cache} events and propagates those to
 * {@link CachePeer} peers of the Cache.
 *
 * @author Greg Luck
 * @version $Id$
 */
public class RMISynchronousCacheReplicator extends AbstractRMICacheReplicator implements CacheReplicator {

    private static final Logger LOG = LoggerFactory.getLogger(RMISynchronousCacheReplicator.class.getName());

    public RMISynchronousCacheReplicator(
            boolean replicatePuts,
            boolean replicatePutsViaCopy,
            boolean replicateUpdates,
            boolean replicateUpdatesViaCopy,
            boolean replicateRemovals) {
        super(replicatePuts, replicatePutsViaCopy, replicateUpdates, replicateUpdatesViaCopy, replicateRemovals);
    }

    @Override
    protected void processEvent(RmiEventMessage eventMessage) {
        for (CachePeer cachePeer : listRemoteCachePeers(eventMessage.getEhcache())) {
            try {
                cachePeer.send(singletonList(eventMessage));
            } catch (Exception e) {
                LOG.error("Exception on replication of message {}. Continuing...", eventMessage, e);
            }
        }
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
        return new RMISynchronousCacheReplicator(
                replicatePuts,
                replicatePutsViaCopy,
                replicateUpdates,
                replicateUpdatesViaCopy,
                replicateRemovals);
    }
}
