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

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An RMI based implementation of <code>CachePeer</code>.
 * <p>
 * This class features a customised RMIClientSocketFactory which enables socket timeouts to be configured.
 *
 * @author Greg Luck
 * @version $Id$
 */
public class RMICachePeer extends UnicastRemoteObject implements CachePeer, Remote {

    private static final Logger LOG = LoggerFactory.getLogger(RMICachePeer.class);

    private final String hostname;
    private final Integer rmiRegistryPort;
    private final Integer remoteObjectPort;
    private final Ehcache cache;

    /**
     * Construct a new remote peer.
     *
     * @param cache               The cache attached to the peer
     * @param hostName            The host name the peer is running on.
     * @param rmiRegistryPort     The port number on which the RMI Registry listens. Should be an unused port in
     *                            the range 1025 - 65536
     * @param remoteObjectPort    the port number on which the remote objects bound in the registry receive calls.
     *                            This defaults to a free port if not specified.
     *                            Should be an unused port in the range 1025 - 65536
     */
    public RMICachePeer(Ehcache cache, String hostName, Integer rmiRegistryPort, Integer remoteObjectPort, Integer socketTimeoutMillis)
            throws RemoteException {
        super(remoteObjectPort, new ConfigurableRMIClientSocketFactory(socketTimeoutMillis),
                ConfigurableRMIClientSocketFactory.getConfiguredRMISocketFactory());

        this.remoteObjectPort = remoteObjectPort;
        this.hostname = hostName;
        this.rmiRegistryPort = rmiRegistryPort;
        this.cache = cache;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation gives an URL which has meaning to the RMI remoting system.
     *
     * @return the URL, without the scheme, as a string e.g. //hostname:port/cacheName
     */
    @Override
    public String getUrl() {

        return getUrlBase() + "/" + cache.getName();
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation gives an URL which has meaning to the RMI remoting system.
     *
     * @return the URL, without the scheme, as a string e.g. //hostname:port
     */
    @Override
    public String getUrlBase() {

        return "//" + (hostname.contains(":") ? ("[" + hostname + "]") : hostname) + ":" + rmiRegistryPort;
    }

    /**
     * Returns a list of all elements in the cache, whether or not they are expired.
     * <p>
     * The returned keys are unique and can be considered as a set.
     * <p>
     * The List returned is not live. It is a copy.
     * <p>
     * The time taken is O(n). On a single cpu 1.8Ghz P4, approximately 8ms is required
     * for each 1000 entries.
     *
     * @return a list of {@link Object} keys
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<Serializable> getKeys() throws RemoteException {

        return ((List<Object>) cache.getKeys())
                .stream()
                .map(key -> (Serializable) key)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Gets an element from the cache, without updating Element statistics. Cache statistics are
     * still updated.
     *
     * @param key a serializable value
     * @return the element, or Optional.empty(), if it does not exist.
     */
    @Override
    public Optional<Element> getQuiet(Serializable key) throws RemoteException {

        return Optional.ofNullable(cache.getQuiet(key));
    }

    /**
     * Gets a list of elements from the cache, for a list of keys, without updating Element statistics. Time to
     * idle lifetimes are therefore not affected.
     * <p>
     * Cache statistics are still updated.
     * <p>
     * Callers should ideally first call this method with a small list of keys to gauge the size of a typical Element.
     * Then a calculation can be made of the right number to request each time to optimise network performance and
     * not cause an OutOfMemory error on this Cache.
     *
     * @param keys a list of serializable values which represent keys
     * @return a list of Elements. If an element was not found or null, it will not be in the list.
     */
    @Override
    public List<Element> getElements(List<Serializable> keys) throws RemoteException {
        if (keys == null) {

            return new ArrayList<>();
        }

        return keys.stream()
                .map(cache::getQuiet)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }


    /**
     * Puts an Element into the underlying cache without notifying listeners or updating statistics.
     */
    @Override
    public void put(Element element) throws RemoteException, IllegalArgumentException, IllegalStateException {
        cache.put(element, true);
        if (LOG.isDebugEnabled()) {
            LOG.debug("RMICachePeer for cache " + cache.getName() + ": remote put received. Element is: " + element);
        }
    }


    /**
     * Removes an Element from the underlying cache without notifying listeners or updating statistics.
     *
     * @return true if the element was removed, false if it was not found in the cache
     */
    @Override
    public boolean remove(Serializable key) throws RemoteException, IllegalStateException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("RMICachePeer for cache " + cache.getName() + ": remote remove received for key: " + key);
        }
        return cache.remove(key, true);
    }

    /**
     * Removes all cached items.
     *
     * @throws IllegalStateException if the cache is not {@link net.sf.ehcache.Status#STATUS_ALIVE}
     */
    @Override
    public void removeAll() throws RemoteException, IllegalStateException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("RMICachePeer for cache " + cache.getName() + ": remote removeAll received");
        }
        cache.removeAll(true);
    }

    /**
     * Send the cache peer with an ordered list of {@link EventMessage}s
     * <p>
     * This enables multiple messages to be delivered in one network invocation.
     */
    @Override
    public void send(List<RmiEventMessage> eventMessages) throws RemoteException {
        for (RmiEventMessage message : eventMessages) {
            switch (message.getType()) {
                case PUT:
                    put(message.getElement());
                    break;
                case REMOVE:
                    remove(message.getSerializableKey());
                    break;
                case REMOVE_ALL:
                    removeAll();
                    break;
                default:
                    LOG.error("Unknown event type: " + message.getType());
                    break;
            }
        }
    }

    /**
     * Gets the cache name
     */
    @Override
    public String getName() throws RemoteException {
        return cache.getName();
    }

    @Override
    public String getGuid() throws RemoteException {
        return cache.getGuid();
    }

    @Override
    public String toString() {
        return "URL: " + getUrl() + " Remote Object Port: " + remoteObjectPort;
    }
}
