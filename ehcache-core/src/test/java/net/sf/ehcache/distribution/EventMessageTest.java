/**
 *  Copyright Terracotta, Inc.
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

package net.sf.ehcache.distribution;


import net.sf.ehcache.AbstractCacheTest;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

import net.sf.ehcache.distribution.RmiEventMessage.RmiEventType;

import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.test.categories.CheckShorts;

/**
 * Tests Serialization and SoftReferences in EventMessage
 *
 * @author Greg Luck
 * @version $Id$
 */
@Category(CheckShorts.class)
@RunWith(MockitoJUnitRunner.class)
public class EventMessageTest {

    private static final Logger LOG = LoggerFactory.getLogger(EventMessageTest.class.getName());

    @Mock
    Ehcache cache;

    /**
     * SoftReference behaviour testing.
     */
    @Test
    public void testSoftReferences() {
        AbstractCacheTest.forceVMGrowth();
        Map<Integer, SoftReference<byte[]>> map = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put(i, new SoftReference<byte[]>(new byte[1000000]));
        }

        int counter = 0;
        for (int i = 0; i < 100; i++) {
            SoftReference<byte[]> softReference = map.get(i);
            byte[] payload = softReference.get();
            if (payload != null) {
                LOG.info("Value found for " + i);
                counter++;
            }
        }

        //This one varies by operating system and architecture.
        assertThat(counter).describedAs("You should get more than 13 out of SoftReferences").isGreaterThan(13);
    }

    /**
     * test serialization and deserialization of EventMessage.
     */
    @Test
    public void testSerializationOfPut() throws IOException, ClassNotFoundException {

        RmiEventMessage sourceEventMessage = new RmiEventMessage(cache, new Element("key", "element"));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(sourceEventMessage);
        oos.close();

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        RmiEventMessage eventMessage = (RmiEventMessage) ois.readObject();
        ois.close();

        //Check after Serialization
        SoftAssertions.assertSoftly((assertions) -> {
            assertions.assertThat(eventMessage.getType()).isEqualTo(RmiEventType.PUT);

            ObjectAssert<Element> elementDiffer = assertions.assertThat(eventMessage.getElement()).describedAs("element differ").isNotNull();
            elementDiffer.extracting(Element::getObjectKey).describedAs("key differ").isEqualTo("key");
            elementDiffer.extracting(Element::getObjectValue).describedAs("value differ").isEqualTo("element");
        });
    }

    /**
     * test serialization and deserialization of EventMessage.
     */
    @Test
    public void testSerializationOfRemove() throws IOException, ClassNotFoundException {

        RmiEventMessage sourceEventMessage = new RmiEventMessage(cache, "key");

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bout);
        oos.writeObject(sourceEventMessage);
        byte[] serializedValue = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue);
        ObjectInputStream ois = new ObjectInputStream(bin);
        RmiEventMessage eventMessage = (RmiEventMessage) ois.readObject();

        //Check after Serialization
        SoftAssertions.assertSoftly((assertions) -> {
            assertions.assertThat(eventMessage.getType()).isEqualTo(RmiEventType.REMOVE);
            assertions.assertThat(eventMessage.getElement()).describedAs("element differ").isNull();
            assertions.assertThat(eventMessage.getSerializableKey()).describedAs("key differ").isEqualTo("key");
        });
    }

    /**
     * test serialization and deserialization of EventMessage.
     */
    @Test
    public void testSerializationOfRemoveAll() throws IOException, ClassNotFoundException {

        RmiEventMessage sourceEventMessage = new RmiEventMessage(cache);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bout);
        oos.writeObject(sourceEventMessage);
        byte[] serializedValue = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue);
        ObjectInputStream ois = new ObjectInputStream(bin);
        RmiEventMessage eventMessage = (RmiEventMessage) ois.readObject();

        //Check after Serialization
        SoftAssertions.assertSoftly((assertions) -> {
            assertions.assertThat(eventMessage.getType()).isEqualTo(RmiEventType.REMOVE_ALL);
            assertions.assertThat(eventMessage.getElement()).describedAs("element differ").isNull();
            assertions.assertThat(eventMessage.getSerializableKey()).describedAs("key differ").isNull();
        });
    }
}
