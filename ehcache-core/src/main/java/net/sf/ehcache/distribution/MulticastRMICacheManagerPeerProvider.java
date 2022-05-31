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
import net.sf.ehcache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.Thread.sleep;

/**
 * A peer provider which discovers peers using Multicast.
 * <p>
 * Hosts can be in three different levels of conformance with the Multicast specification (RFC1112), according to the requirements they meet.
 * <ol>
 * <li>Level 0 is the "no support for IP Multicasting" level. Lots of hosts and routers in the Internet are in this state,
 * as multicast support is not mandatory in IPv4 (it is, however, in IPv6).
 * Not too much explanation is needed here: hosts in this level can neither send nor receive multicast packets.
 * They must ignore the ones sent by other multicast capable hosts.
 * <li>Level 1 is the "support for sending but not receiving multicast IP datagrams" level.
 * Thus, note that it is not necessary to join a multicast group to be able to send datagrams to it.
 * Very few additions are needed in the IP module to make a "Level 0" host "Level 1-compliant".
 * <li>Level 2 is the "full support for IP multicasting" level.
 * Level 2 hosts must be able to both send and receive multicast traffic.
 * They must know the way to join and leave multicast groups and to propagate this information to multicast routers.
 * Thus, they must include an Internet Group Management Protocol (IGMP) implementation in their TCP/IP stack.
 * </ol>
 * <p>
 * The list of CachePeers is maintained via heartbeats. rmiUrls are looked up using RMI and converted to CachePeers on
 * registration. On lookup any stale references are removed.
 *
 * @author Greg Luck
 * @version $Id$
 */
public class MulticastRMICacheManagerPeerProvider extends RMICacheManagerPeerProvider implements CacheManagerPeerProvider {

    private static final String STATE_CHECKER_THREAD_NAME = "State Checker Thread";
    private static final Logger LOG = LoggerFactory.getLogger(MulticastRMICacheManagerPeerProvider.class);

    /**
     * Contains a RMI URLs of the form: "//" + hostName + ":" + port + "/" + cacheName as key
     */
    private final Map<String, CachePeerState> activePeerUrls = Collections.synchronizedMap(new HashMap<>());

    private final MulticastKeepaliveHeartbeatReceiver heartBeatReceiver;
    private final MulticastKeepaliveHeartbeatSender heartBeatSender;
    private final Thread staleCheckerThread = new Thread(null, this::rmiUrlsStateChecker, STATE_CHECKER_THREAD_NAME);


    /**
     * Creates and starts a multicast peer provider
     *
     * @param groupMulticastAddress 224.0.0.1 to 239.255.255.255 e.g. 230.0.0.1
     * @param groupMulticastPort    1025 to 65536 e.g. 4446
     * @param hostAddress           the address of the interface to use for sending and receiving multicast. May be null.
     */
    public MulticastRMICacheManagerPeerProvider(
            CacheManager cacheManager,
            InetAddress groupMulticastAddress,
            Integer groupMulticastPort,
            Integer timeToLive,
            InetAddress hostAddress) {
        super(cacheManager);

        heartBeatReceiver = new MulticastKeepaliveHeartbeatReceiver(this, groupMulticastAddress, groupMulticastPort, hostAddress);
        heartBeatSender = new MulticastKeepaliveHeartbeatSender(this, groupMulticastAddress, groupMulticastPort, timeToLive, hostAddress);
        staleCheckerThread.setDaemon(true);
    }

    /**
     * Register a new peer, but only if the peer is new, otherwise the last seen timestamp is updated.
     * <p>
     * This method is thread-safe. It relies on peerUrls being a synchronizedMap
     *
     * @param rmiUrl the URL to register
     */
    public synchronized void registerPeer(String rmiUrl) {
        CachePeerState cachePeerState = activePeerUrls.get(rmiUrl);
        if (cachePeerState == null) {
            // not known before

            activePeerUrls.put(rmiUrl, new CachePeerState(rmiUrl));
        } else {
            // record last seen
            cachePeerState.touch();
        }
    }

    public synchronized void unregisterPeer(String rmiUrl) {
        activePeerUrls.remove(rmiUrl);
    }

    @Override
    synchronized Set<String> getRegisteredRmiUrls() {
        return new TreeSet<>(activePeerUrls.keySet());
    }

    void rmiUrlsStateChecker() {
        while (true) {
            try {
                sleep(MulticastKeepaliveHeartbeatSender.getHeartBeatInterval());

                checkActivePeerEntries(activePeerUrls.entrySet());
            } catch (InterruptedException e) {
                LOG.info("{} interrupted, quitting", STATE_CHECKER_THREAD_NAME);
                return;
            }
        }
    }

    synchronized void checkActivePeerEntries(Set<Entry<String, CachePeerState>> entries) {
        for (Iterator<Entry<String, CachePeerState>> iterator = entries.iterator(); iterator.hasNext(); ) {
            Entry<String, CachePeerState> entry = iterator.next();

            PeerState peerState = entry.getValue().peerState();
            if (peerState == PeerState.STALE) {
                LOG.warn("peer entry {} is {} and will be removed from peer list", entry.getKey(), peerState);
                iterator.remove();
            }
        }
    }

    @Override
    public void dispose() {
        heartBeatSender.dispose();
        heartBeatReceiver.dispose();
        staleCheckerThread.interrupt();
    }

    @Override
    public void init() {
        try {
            staleCheckerThread.start();
            heartBeatReceiver.init();
            heartBeatSender.init();
        } catch (IOException e) {
            throw new CacheException("could not initialize peer provider", e);
        }
    }

    /**
     * Time for a cluster to form. This varies considerably, depending on the implementation.
     *
     * @return the time in ms, for a cluster to form
     */
    public long getTimeForClusterToForm() {
        return getStaleTime();
    }

    /**
     * The time after which an unrefreshed peer provider entry is considered stale.
     */
    long getStaleTime() {
        return MulticastKeepaliveHeartbeatSender.getHeartBeatStaleTime();
    }

    enum PeerState {
        HEALTHY,
        UNHEALTHY,
        STALE
    }

    /**
     * Entry containing a looked up CachePeer and date
     */
    class CachePeerState {

        private final String rmiUrl;

        /**
         * last access date.
         */
        private Date lastAccess;

        /**
         * date when the entry got stale.
         */
        private Date staleSince;

        /**
         * @param rmiUrl the RMI URL of the peer
         */
        CachePeerState(String rmiUrl) {
            this.rmiUrl = rmiUrl;
            this.lastAccess = new Date();
        }

        void touch() {
            this.lastAccess = new Date();
            if (staleSince != null) {
                LOG.info("rmiUrl {} has changed to {} state", rmiUrl, PeerState.HEALTHY);
                this.staleSince = null;
            }
        }

        boolean isNotStale() {
            boolean stale = lastAccess.getTime() < (System.currentTimeMillis() - getStaleTime());

            if (stale && this.staleSince == null) {
                LOG.warn("rmiUrl {} has changed to {} state", rmiUrl, PeerState.UNHEALTHY);
                this.staleSince = new Date();
            }

            return this.staleSince == null;
        }

        /**
         * Whether the entry should be considered stale.
         * This will depend on the type of RMICacheManagerPeerProvider.
         * This method should be overridden for implementations that go stale based on date
         *
         * @return true if stale for a longer time
         */
        PeerState peerState() {
            if (isNotStale()) {
                return PeerState.HEALTHY;
            }

            return staleSince.before(getExpireDate()) ? PeerState.STALE : PeerState.UNHEALTHY;
        }

        Date getExpireDate() {
            Calendar instance = Calendar.getInstance();
            instance.add(Calendar.MILLISECOND, -getExpireDuration());
            return instance.getTime();
        }
    }

    int getExpireDuration() {
        return (((int) MulticastKeepaliveHeartbeatSender.getHeartBeatInterval()) * 12);
    }

    /**
     * @return the MulticastKeepaliveHeartbeatReceiver
     */
    @SuppressWarnings("unused")
    public MulticastKeepaliveHeartbeatReceiver getHeartBeatReceiver() {
        return heartBeatReceiver;
    }

    /**
     * @return the MulticastKeepaliveHeartbeatSender
     */
    public MulticastKeepaliveHeartbeatSender getHeartBeatSender() {
        return heartBeatSender;
    }
}
