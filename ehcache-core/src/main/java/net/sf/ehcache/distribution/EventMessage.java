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

import java.io.Serializable;
import java.util.Objects;

import net.sf.ehcache.Ehcache;

/**
 * An Event Message, in respect of a particular cache.
 * <p>
 * The message is Serializable, so that it can be sent across the network.
 * <p>
 * The value of an Element is referenced with a SoftReference, so that a
 * value will fail to be delivered in preference to an OutOfMemory error.
 *
 * @author Greg Luck
 * @version $Id$
 */
public class EventMessage implements Serializable {

    private static final long serialVersionUID = -293616939110963630L;

    /**
     * The key component.
     */
    private final Serializable key;

    /**
     * The associated cache.
     */
    private final transient Ehcache cache;

    /**
     * @param cache the source cache
     * @param key   a key of a cache object
     */
    public EventMessage(Ehcache cache, Serializable key) {
        Objects.requireNonNull(cache, "cache not set");
        this.cache = cache;
        this.key = key;
    }

    /**
     * Gets the associated {@code Ehcache}.
     *
     * @return the associated cache
     */
    public final Ehcache getEhcache() {
        return cache;
    }

    /**
     * @return the key component of the message. null if a PUT event
     */
    public final Serializable getSerializableKey() {
        return key;
    }
}
