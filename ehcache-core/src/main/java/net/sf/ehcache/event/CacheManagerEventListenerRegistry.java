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

package net.sf.ehcache.event;

import net.sf.ehcache.Status;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Registered listeners for registering and unregistering CacheManagerEventListeners and sending notifications to registrants.
 * <p>
 * There is one of these per CacheManager. It is a composite listener.
 *
 * @author Greg Luck
 * @version $Id$
 * @since 1.3
 */
public class CacheManagerEventListenerRegistry implements CacheManagerEventListener {

    private volatile Status status;

    /**
     * A Set of CacheEventListeners keyed by listener instance.
     * CacheEventListener implementations that will be notified of this cache's events.
     *
     * @see CacheManagerEventListener
     */
    private final Set<CacheManagerEventListener> listeners;

    /**
     * Construct a new registry
     */
    public CacheManagerEventListenerRegistry() {
        status = Status.STATUS_UNINITIALISED;
        listeners = new CopyOnWriteArraySet<>();
    }

    /**
     * Adds a listener to the notification service. No guarantee is made that listeners will be
     * notified in the order they were added.
     *
     * @param cacheManagerEventListener the listener to add. Can be null, in which case nothing happens
     * @return true if the listener is being added and was not already added
     */
    public boolean registerListener(CacheManagerEventListener cacheManagerEventListener) {
        if (cacheManagerEventListener == null) {
            return false;
        }
        return listeners.add(cacheManagerEventListener);
    }

    /**
     * Removes a listener from the notification service.
     *
     * @param cacheManagerEventListener the listener to remove
     * @return true if the listener was present
     */
    public boolean unregisterListener(CacheManagerEventListener cacheManagerEventListener) {
        return listeners.remove(cacheManagerEventListener);
    }

    /**
     * Initialises the listeners, ready to receive events.
     */
    public void init() {
        listeners.forEach(CacheManagerEventListener::init);
        status = Status.STATUS_ALIVE;
    }

    /**
     * Tell listeners to dispose themselves.
     * Because this method is only called from a synchronized cache method, it does not itself need to be
     * synchronized.
     */
    public void dispose() {
        listeners.forEach(CacheManagerEventListener::dispose);
        listeners.clear();
        status = Status.STATUS_SHUTDOWN;
    }

    /**
     * Returns the listener status.
     *
     * @return the status at the point in time the method is called
     */
    public Status getStatus() {
        return status;
    }

    public void notifyCacheAdded(String cacheName) {
        for (CacheManagerEventListener cacheManagerEventListener : listeners) {
            cacheManagerEventListener.notifyCacheAdded(cacheName);
        }
    }

    public void notifyCacheRemoved(String cacheName) {
        for (CacheManagerEventListener cacheManagerEventListener : listeners) {
            cacheManagerEventListener.notifyCacheRemoved(cacheName);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(" cacheManagerEventListeners: ");
        for (CacheManagerEventListener cacheManagerEventListener : listeners) {
            sb.append(cacheManagerEventListener.getClass().getName()).append(" ");
        }
        return sb.toString().trim();
    }
}
