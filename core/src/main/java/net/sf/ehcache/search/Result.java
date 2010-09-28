/**
 *  Copyright 2003-2010 Terracotta, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.sf.ehcache.search;

/**
 * Represents a single cache entry that has been selected by a cache query
 * 
 * @author teck
 * @author Greg Luck
 */
public interface Result {

    /**
     * Return the key for this cache entry.
     * 
     * @return key object (never null)
     * @throws UnsupportedOperationException
     *             if keys were not selected by the originating query
     */
    Object getKey() throws UnsupportedOperationException;

    /**
     * Return the value object for this cache entry. Upon every call, this
     * method performs a get() on the underlying Cache for this entry's key.
     * 
     * If the query results were summarised with an Aggregator, the result will be
     * present in Value
     * 
     * @return value object (which might be null if this entry no longer exists
     *         in the cache)
     * @throws UnsupportedOperationException
     *             if keys were not selected by the originating query
     */
    Object getValue() throws UnsupportedOperationException;

    /**
     * Retrieve the given attribute value for this cache entry
     * 
     * @param attribute
     *            the attribute to retrieve
     * @return the attribute value, or null if there is none
     * @throws UnsupportedOperationException
     *             if the given attribute was not explicitly selected by the
     *             originating query
     */
    <T> T getAttribute(Attribute<T> attribute) throws UnsupportedOperationException;
}
