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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Sends heartbeats to a multicast group containing a compressed list of URLs.
 * <p>
 * You can control how far the multicast packets propagate by setting the badly misnamed "TTL".
 * Using the multicast IP protocol, the TTL value indicates the scope or range in which a packet may be forwarded.
 * By convention:
 * <ul>
 * <li>0 is restricted to the same host
 * <li>1 is restricted to the same subnet
 * <li>32 is restricted to the same site
 * <li>64 is restricted to the same region
 * <li>128 is restricted to the same continent
 * <li>255 is unrestricted
 * </ul>
 * You can also control how often the heartbeat sends by setting the interval.
 *
 * @author Greg Luck
 * @version $Id$
 */
public class MulticastKeepaliveHeartbeatSender {

    private static final Logger LOG = LoggerFactory.getLogger(MulticastKeepaliveHeartbeatSender.class);

    private static final int DEFAULT_HEARTBEAT_INTERVAL = 5000;
    private static final int MINIMUM_HEARTBEAT_INTERVAL = 1000;
    private static final int MAXIMUM_PEERS_PER_SEND = 150;
    private static final int ONE_HUNDRED_MS = 100;

    private static long heartBeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private static long heartBeatStaleTime = -1;

    private final InetAddress groupMulticastAddress;
    private final Integer groupMulticastPort;
    private final Integer timeToLive;
    private final MulticastServerThread serverThread = new MulticastServerThread();
    private final RMICacheManagerPeerProvider peerProvider;
    private final InetAddress hostAddress;

    private volatile boolean stopped;

    /**
     * @param timeToLive See class description for the meaning of this parameter.
     */
    public MulticastKeepaliveHeartbeatSender(
            RMICacheManagerPeerProvider peerProvider,
            InetAddress multicastAddress,
            Integer multicastPort,
            Integer timeToLive,
            InetAddress hostAddress) {
        this.peerProvider = peerProvider;
        this.groupMulticastAddress = multicastAddress;
        this.groupMulticastPort = multicastPort;
        this.timeToLive = timeToLive;
        this.hostAddress = hostAddress;

    }

    /**
     * Start the heartbeat thread
     */
    public void init() {
        serverThread.start();
    }

    /**
     * Shutdown this heartbeat sender
     */
    public synchronized void dispose() {
        stopped = true;
        notifyAll();
        serverThread.interrupt();
    }

    /**
     * A thread which sends a multicast heartbeat every second
     */
    private class MulticastServerThread extends Thread {

        private MulticastSocket socket;
        private List<byte[]> compressedUrlListList = new ArrayList<>();
        private int cachePeersHash;


        /**
         * Constructor
         */
        public MulticastServerThread() {
            super("Multicast Heartbeat Sender Thread");
            setDaemon(true);
        }

        @SuppressWarnings("BusyWait")
        @Override
        public void run() {
            while (!stopped) {
                try {
                    socket = new MulticastSocket(groupMulticastPort);
                    if (hostAddress != null) {
                        socket.setInterface(hostAddress);
                    }
                    socket.setTimeToLive(timeToLive);
                    socket.joinGroup(groupMulticastAddress);

                    while (!stopped) {
                        for (byte[] buffer : createCachePeersPayload()) {
                            socket.send(new DatagramPacket(buffer, buffer.length, groupMulticastAddress, groupMulticastPort));
                        }
                        try {
                            synchronized (this) {
                                wait(heartBeatInterval);
                            }
                        } catch (InterruptedException e) {
                            if (!stopped) {
                                LOG.error("Sleep after send interrupted.", e);
                            }
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Error on multicast socket", e);
                } catch (Throwable e) {
                    LOG.error("Unexpected throwable in run thread. Continuing...", e);
                } finally {
                    closeSocket();
                }
                if (!stopped) {
                    try {
                        sleep(heartBeatInterval);
                    } catch (InterruptedException e) {
                        LOG.error("Sleep after error interrupted.", e);
                    }
                }
            }
        }

        /**
         * Creates a gzipped payload.
         * <p>
         * The last gzipped payload is retained and only recalculated if the list of cache peers
         * has changed.
         *
         * @return a gzipped byte[]
         */
        private List<byte[]> createCachePeersPayload() {

            CacheManagerPeerListener cacheManagerPeerListener = peerProvider.getCachePeerListener();
            if (cacheManagerPeerListener == null) {
                LOG.warn("The RMICacheManagerPeerListener is missing. You need to configure a cacheManagerPeerListenerFactory" +
                        " with class=\"net.sf.ehcache.distribution.RMICacheManagerPeerListenerFactory\" in ehcache.xml.");
                return new ArrayList<>();
            }
            List<CachePeer> localCachePeers = cacheManagerPeerListener.getBoundCachePeers();
            int newCachePeersHash = localCachePeers.hashCode();
            if (cachePeersHash != newCachePeersHash) {
                cachePeersHash = newCachePeersHash;
                compressedUrlListList = PayloadUtil.createCompressedPayloadList(localCachePeers, MAXIMUM_PEERS_PER_SEND);
            }
            return compressedUrlListList;
        }

        @Override
        public void interrupt() {
            closeSocket();
            super.interrupt();
        }

        private void closeSocket() {
            try {
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.leaveGroup(groupMulticastAddress);
                    } catch (IOException e) {
                        LOG.error("Error leaving multicast group. Message was " + e.getMessage());
                    }
                    socket.close();
                }
            } catch (NoSuchMethodError e) {
                LOG.debug("socket.isClosed is not supported by JDK1.3");
                try {
                    socket.leaveGroup(groupMulticastAddress);
                } catch (IOException ex) {
                    LOG.error("Error leaving multicast group. Message was " + ex.getMessage());
                }
                socket.close();
            }
        }

    }

    /**
     * Sets the heartbeat interval to something other than the default of 5000ms. This is useful for testing,
     * but not recommended for production. This method is static and so affects the heartbeat interval of all
     * senders. The change takes effect after the next scheduled heartbeat.
     *
     * @param heartBeatInterval a time in ms, greater than 1000
     */
    public static void setHeartBeatInterval(long heartBeatInterval) {
        if (heartBeatInterval < MINIMUM_HEARTBEAT_INTERVAL) {
            LOG.warn("Trying to set heartbeat interval too low. Using MINIMUM_HEARTBEAT_INTERVAL instead.");
            MulticastKeepaliveHeartbeatSender.heartBeatInterval = MINIMUM_HEARTBEAT_INTERVAL;
        } else {
            MulticastKeepaliveHeartbeatSender.heartBeatInterval = heartBeatInterval;
        }
    }

    /**
     * Sets the heartbeat stale time to something other than the default of {@code ((2 * HeartBeatInterval) + 100)ms}.
     * This is useful for testing, but not recommended for production. This method is static and so affects the stale
     * time all users.
     *
     * @param heartBeatStaleTime a time in ms
     */
    public static void setHeartBeatStaleTime(long heartBeatStaleTime) {
        MulticastKeepaliveHeartbeatSender.heartBeatStaleTime = heartBeatStaleTime;
    }

    /**
     * Returns the heartbeat interval.
     */
    public static long getHeartBeatInterval() {
        return heartBeatInterval;
    }

    /**
     * Returns the time after which a heartbeat is considered stale.
     */
    public static long getHeartBeatStaleTime() {
        if (heartBeatStaleTime < 0) {
            return (heartBeatInterval * 2) + ONE_HUNDRED_MS;
        } else {
            return heartBeatStaleTime;
        }
    }

    /**
     * @return the TTL
     */
    public Integer getTimeToLive() {
        return timeToLive;
    }
}
