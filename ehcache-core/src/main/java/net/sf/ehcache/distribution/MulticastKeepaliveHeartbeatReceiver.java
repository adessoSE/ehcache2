/**
 * Copyright Terracotta, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * <a href="http://www.apache.org/licenses/LICENSE-2.0">http://www.apache.org/licenses/LICENSE-2.0</a>
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.sf.ehcache.distribution;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sf.ehcache.util.NamedThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives heartbeats from any {@link MulticastKeepaliveHeartbeatSender}s out there.
 * <p>
 * Our own multicast heartbeats are ignored.
 *
 * @author Greg Luck
 * @version $Id$
 */
public class MulticastKeepaliveHeartbeatReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(MulticastKeepaliveHeartbeatReceiver.class);

    private final ExecutorService processingThreadPool;
    private final Thread receiverThread = new Thread(null, this::receiveHeartBeats, "Multicast Heartbeat Receiver Thread");
    private final Set<String> rmiUrlsProcessingQueue = Collections.synchronizedSet(new HashSet<>());

    private final InetAddress groupMulticastAddress;
    private final Integer groupMulticastPort;
    private final InetAddress hostAddress;

    private final MulticastRMICacheManagerPeerProvider peerProvider;
    private MulticastSocket socket;

    private volatile boolean stopped;

    /**
     * @param peerProvider     the parent peer provider
     * @param multicastAddress the multicast address
     * @param multicastPort    the multicast port
     * @param hostAddress      the host address of the interface to bind to
     */
    public MulticastKeepaliveHeartbeatReceiver(MulticastRMICacheManagerPeerProvider peerProvider, InetAddress multicastAddress, Integer multicastPort, InetAddress hostAddress) {
        this.peerProvider = peerProvider;
        this.groupMulticastAddress = multicastAddress;
        this.groupMulticastPort = multicastPort;
        this.hostAddress = hostAddress;
        this.processingThreadPool = Executors.newCachedThreadPool(new NamedThreadFactory("Multicast keep-alive Heartbeat Receiver"));
        this.receiverThread.setDaemon(true);
    }

    void init() throws IOException {
        openSocket();
        receiverThread.start();
    }

    /**
     * Shutdown the heartbeat.
     */
    public void dispose() {
        LOG.debug("dispose called");
        processingThreadPool.shutdownNow();
        stopped = true;
        closeSocket();
        receiverThread.interrupt();
    }

    void openSocket() throws IOException {
        socket = new MulticastSocket(groupMulticastPort);
        if (hostAddress != null) {
            socket.setInterface(hostAddress);
        }
        socket.joinGroup(groupMulticastAddress);
    }

    void closeSocket() {
        try {
            socket.leaveGroup(groupMulticastAddress);
        } catch (IOException e) {
            LOG.error("Error leaving group");
        }
        socket.close();
    }

    void receiveHeartBeats() {
        byte[] buf = new byte[PayloadUtil.MTU];
        try {
            while (!stopped) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(packet);
                    processPayload(packet.getData());
                } catch (IOException e) {
                    if (!stopped) {
                        LOG.error("Error receiving heartbeat.", e);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.error("Multicast receiver thread caught throwable. Cause was " + t.getMessage() + ". Continuing...");
        }
    }

    void processPayload(byte[] compressedPayload) throws IOException {
        String rmiUrls = getRmiUrls(compressedPayload);
        if (isSelf(rmiUrls)) {
            return;
        }

        LOG.debug("rmiUrls received {}", rmiUrls);
        processRmiUrls(rmiUrls);
    }

    String getRmiUrls(byte[] compressedPayload) throws IOException {
        return new String(PayloadUtil.ungzip(compressedPayload)).trim();
    }

    /**
     * This method forks a new executor to process the received heartbeat in a thread pool.
     * That way each remote cache manager cannot interfere with others.
     * <p>
     * In the worst case, we have as many concurrent threads as remote cache managers.
     *
     * @param rmiUrls the urls received from an eventually remote host.
     */
    synchronized void processRmiUrls(final String rmiUrls) {

        if (rmiUrlsProcessingQueue.contains(rmiUrls)) {
            LOG.warn("We are already processing these rmiUrls. Another heartbeat came before we maybe finished: {}", rmiUrls);
            return;
        }

        // Add the rmiUrls we are processing.
        rmiUrlsProcessingQueue.add(rmiUrls);

        processingThreadPool.execute(() -> {
            try {
                for (String rmiUrl : rmiUrls.split(PayloadUtil.URL_DELIMITER_REGEXP)) {
                    if (stopped) {
                        return;
                    }
                    peerProvider.registerPeer(rmiUrl);
                }
            } finally {
                // Remove the rmiUrls we just processed
                rmiUrlsProcessingQueue.remove(rmiUrls);
            }
        });
    }


    /**
     * @param rmiUrls the list of rmiUrls
     * @return true if our own hostname and listener port are found in the list. This means we have
     * caught our onw multicast, and it should be ignored.
     */
    boolean isSelf(String rmiUrls) {
        CacheManagerPeerListener cacheManagerPeerListener = peerProvider.getCacheManager().getCachePeerListener("RMI");
        if (cacheManagerPeerListener == null) {
            return false;
        }

        try {
            Optional<CachePeer> peer = cacheManagerPeerListener.getBoundCachePeers().stream().findFirst();
            if (!peer.isPresent()) {
                return false;
            }

            return rmiUrls.contains(peer.get().getUrlBase());
        } catch (RemoteException e) {
            LOG.error("Error getting URL base", e);
            return false;
        }
    }
}
