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

import java.io.Serializable;
import java.util.Objects;

/**
 * @author cdennis
 */
public class RmiEventMessage extends EventMessage {

    /**
     * Enumeration of event types.
     */
    public enum RmiEventType {

        /**
         * A put or update event.
         */
        PUT,

        /**
         * A remove or invalidate event.
         */
        REMOVE,

        /**
         * A removeAll, which removes all elements from a cache
         */
        REMOVE_ALL
    }

    /**
     * The event component.
     */
    private final RmiEventType type;

    /**
     * The element component.
     */
    private final Element element;

    /**
     * @param cache   source cache of the event
     * @param type    type of event
     * @param key     key in cache
     * @param element element in cache
     */
    private RmiEventMessage(Ehcache cache, RmiEventType type, Serializable key, Element element) {
        super(cache, key);
        Objects.requireNonNull(type, "type not set.");
        this.type = type;
        this.element = element;
    }

    /**
     * RmiEventMessage of remove all
     */
    public RmiEventMessage(Ehcache cache) {
        this(cache, RmiEventType.REMOVE_ALL, null, null);
    }

    /**
     * RmiEventMessage of remove
     */
    public RmiEventMessage(Ehcache cache, Serializable key) {
        this(cache, RmiEventType.REMOVE, key, null);
    }

    /**
     * RmiEventMessage of put
     */
    public RmiEventMessage(Ehcache cache, Element element) {
        this(cache, RmiEventType.PUT, null, element);
    }

    /**
     * @return the event type.
     */
    public RmiEventType getType() {
        return type;
    }

    /**
     * @return the element component of the message. null if a REMOVE event
     */
    public Element getElement() {
        return element;
    }

    public boolean isSerializable() {
        switch (type) {
            case PUT:
                return element.isSerializable();
            case REMOVE:
            case REMOVE_ALL:
                return true;
            default:
                return false;
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [" +
                "type = " + this.type +
                ", key = " + this.getSerializableKey() +
                ", element = " + this.element +
                ", cache = " + this.getEhcache().getName() +
                "]";
    }
}
