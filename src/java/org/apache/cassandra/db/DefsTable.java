/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * SCHEMA_{KEYSPACES, COLUMNFAMILIES, COLUMNS}_CF are used to store Keyspace/ColumnFamily attributes to make schema
 * load/distribution easy, it replaces old mechanism when local migrations where serialized, stored in system.Migrations
 * and used for schema distribution.
 *
 * SCHEMA_KEYSPACES_CF layout:
 *
 * <key (AsciiType)>
 *   ascii => json_serialized_value
 *   ...
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks".
 *
 * SCHEMA_COLUMNFAMILIES_CF layout:
 *
 * <key (AsciiType)>
 *     composite(ascii, ascii) => json_serialized_value
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks"., first component of the column name is name of the ColumnFamily, last
 * component is the name of the ColumnFamily attribute.
 *
 * SCHEMA_COLUMNS_CF layout:
 *
 * <key (AsciiType)>
 *     composite(ascii, ascii, ascii) => json_serialized value
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks".
 *
 * Column names where made composite to support 3-level nesting which represents following structure:
 * "ColumnFamily name":"column name":"column attribute" => "value"
 *
 * Example of schema (using CLI):
 *
 * schema_keyspaces
 * ----------------
 * RowKey: ks
 *  => (column=durable_writes, value=true, timestamp=1327061028312185000)
 *  => (column=name, value="ks", timestamp=1327061028312185000)
 *  => (column=replication_factor, value=0, timestamp=1327061028312185000)
 *  => (column=strategy_class, value="org.apache.cassandra.locator.NetworkTopologyStrategy", timestamp=1327061028312185000)
 *  => (column=strategy_options, value={"datacenter1":"1"}, timestamp=1327061028312185000)
 *
 * schema_columnfamilies
 * ---------------------
 * RowKey: ks
 *  => (column=cf:bloom_filter_fp_chance, value=0.0, timestamp=1327061105833119000)
 *  => (column=cf:caching, value="NONE", timestamp=1327061105833119000)
 *  => (column=cf:column_type, value="Standard", timestamp=1327061105833119000)
 *  => (column=cf:comment, value="ColumnFamily", timestamp=1327061105833119000)
 *  => (column=cf:default_validation_class, value="org.apache.cassandra.db.marshal.BytesType", timestamp=1327061105833119000)
 *  => (column=cf:gc_grace_seconds, value=864000, timestamp=1327061105833119000)
 *  => (column=cf:id, value=1000, timestamp=1327061105833119000)
 *  => (column=cf:key_alias, value="S0VZ", timestamp=1327061105833119000)
 *  ... part of the output omitted.
 *
 * schema_columns
 * --------------
 * RowKey: ks
 *  => (column=cf:c:index_name, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:index_options, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:index_type, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:name, value="aGVsbG8=", timestamp=1327061105833119000)
 *  => (column=cf:c:validation_class, value="org.apache.cassandra.db.marshal.AsciiType", timestamp=1327061105833119000)
 */
public class DefsTable
{
    /* saves keyspace definitions to system schema columnfamilies */
    public static synchronized void save(Collection<KSMetaData> keyspaces)
    {
        long timestamp = System.currentTimeMillis();

        for (KSMetaData ksMetaData : keyspaces)
            ksMetaData.toSchema(timestamp).apply();
    }

    /**
     * Load keyspace definitions for the system keyspace (system.SCHEMA_KEYSPACES_CF)
     *
     * @return Collection of found keyspace definitions
     */
    public static Collection<KSMetaData> loadFromTable()
    {
        List<Row> serializedSchema = SystemTable.serializedSchema(SystemTable.SCHEMA_KEYSPACES_CF);

        List<KSMetaData> keyspaces = new ArrayList<KSMetaData>(serializedSchema.size());

        for (Row row : serializedSchema)
        {
            if (Schema.invalidSchemaRow(row) || Schema.ignoredSchemaRow(row))
                continue;

            keyspaces.add(KSMetaData.fromSchema(row, serializedColumnFamilies(row.key)));
        }

        return keyspaces;
    }

    public static ByteBuffer searchComposite(String name, boolean start)
    {
        assert name != null;
        ByteBuffer nameBytes = UTF8Type.instance.decompose(name);
        int length = nameBytes.remaining();
        byte[] bytes = new byte[2 + length + 1];
        bytes[0] = (byte)((length >> 8) & 0xFF);
        bytes[1] = (byte)(length & 0xFF);
        ByteBufferUtil.arrayCopy(nameBytes, 0, bytes, 2, length);
        bytes[bytes.length - 1] = (byte)(start ? 0 : 1);
        return ByteBuffer.wrap(bytes);
    }

    private static Row serializedColumnFamilies(DecoratedKey ksNameKey)
    {
        ColumnFamilyStore cfsStore = SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNFAMILIES_CF);
        return new Row(ksNameKey, cfsStore.getColumnFamily(QueryFilter.getIdentityFilter(ksNameKey, SystemTable.SCHEMA_COLUMNFAMILIES_CF)));
    }

    /**
     * Merge remote schema in form of row mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public static synchronized void mergeSchema(Collection<RowMutation> mutations) throws ConfigurationException, IOException
    {
        // current state of the schema
        Map<DecoratedKey, ColumnFamily> oldKeyspaces = SystemTable.getSchema(SystemTable.SCHEMA_KEYSPACES_CF);
        Map<DecoratedKey, ColumnFamily> oldColumnFamilies = SystemTable.getSchema(SystemTable.SCHEMA_COLUMNFAMILIES_CF);

        for (RowMutation mutation : mutations)
            mutation.apply();

        if (!StorageService.instance.isClientMode())
            flushSchemaCFs();

        Schema.instance.updateVersionAndAnnounce();

        // with new data applied
        Map<DecoratedKey, ColumnFamily> newKeyspaces = SystemTable.getSchema(SystemTable.SCHEMA_KEYSPACES_CF);
        Map<DecoratedKey, ColumnFamily> newColumnFamilies = SystemTable.getSchema(SystemTable.SCHEMA_COLUMNFAMILIES_CF);

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeColumnFamilies(oldColumnFamilies, newColumnFamilies);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (String keyspaceToDrop : keyspacesToDrop)
            dropKeyspace(keyspaceToDrop);

    }

    private static Set<String> mergeKeyspaces(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
    {
        // calculate the difference between old and new states (note that entriesOnlyLeft() will be always empty)
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        /**
         * At first step we check if any new keyspaces were added.
         */
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily ksAttrs = entry.getValue();

            // we don't care about nested ColumnFamilies here because those are going to be processed separately
            if (!(ksAttrs.getColumnCount() == 0))
                addKeyspace(KSMetaData.fromSchema(new Row(entry.getKey(), entry.getValue()), Collections.<CFMetaData>emptyList()));
        }

        /**
         * At second step we check if there were any keyspaces re-created, in this context
         * re-created means that they were previously deleted but still exist in the low-level schema as empty keys
         */

        Map<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntries = diff.entriesDiffering();

        // instead of looping over all modified entries and skipping processed keys all the time
        // we would rather store "left to process" items and iterate over them removing already met keys
        List<DecoratedKey> leftToProcess = new ArrayList<DecoratedKey>(modifiedEntries.size());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : modifiedEntries.entrySet())
        {
            ColumnFamily prevValue = entry.getValue().leftValue();
            ColumnFamily newValue = entry.getValue().rightValue();

            if (prevValue.getColumnCount() == 0)
            {
                addKeyspace(KSMetaData.fromSchema(new Row(entry.getKey(), newValue), Collections.<CFMetaData>emptyList()));
                continue;
            }

            leftToProcess.add(entry.getKey());
        }

        if (leftToProcess.size() == 0)
            return Collections.emptySet();

        /**
         * At final step we updating modified keyspaces and saving keyspaces drop them later
         */

        Set<String> keyspacesToDrop = new HashSet<String>();

        for (DecoratedKey key : leftToProcess)
        {
            MapDifference.ValueDifference<ColumnFamily> valueDiff = modifiedEntries.get(key);

            ColumnFamily newState = valueDiff.rightValue();

            if (newState.getColumnCount() == 0)
                keyspacesToDrop.add(AsciiType.instance.getString(key.key));
            else
                updateKeyspace(KSMetaData.fromSchema(new Row(key, newState), Collections.<CFMetaData>emptyList()));
        }

        return keyspacesToDrop;
    }

    private static void mergeColumnFamilies(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
    {
        // calculate the difference between old and new states (note that entriesOnlyLeft() will be always empty)
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        // check if any new Keyspaces with ColumnFamilies were added.
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily cfAttrs = entry.getValue();

            if (!(cfAttrs.getColumnCount() == 0))
            {
               Map<String, CFMetaData> cfDefs = KSMetaData.deserializeColumnFamilies(new Row(entry.getKey(), cfAttrs));

                for (CFMetaData cfDef : cfDefs.values())
                    addColumnFamily(cfDef);
            }
        }

        // deal with modified ColumnFamilies (remember that all of the keyspace nested ColumnFamilies are put to the single row)
        Map<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntries = diff.entriesDiffering();

        for (DecoratedKey keyspace : modifiedEntries.keySet())
        {
            MapDifference.ValueDifference<ColumnFamily> valueDiff = modifiedEntries.get(keyspace);

            ColumnFamily prevValue = valueDiff.leftValue(); // state before external modification
            ColumnFamily newValue = valueDiff.rightValue(); // updated state

            Row newRow = new Row(keyspace, newValue);

            if (prevValue.getColumnCount() == 0) // whole keyspace was deleted and now it's re-created
            {
                for (CFMetaData cfm : KSMetaData.deserializeColumnFamilies(newRow).values())
                    addColumnFamily(cfm);
            }
            else if (newValue.getColumnCount() == 0) // whole keyspace is deleted
            {
                for (CFMetaData cfm : KSMetaData.deserializeColumnFamilies(new Row(keyspace, prevValue)).values())
                    dropColumnFamily(cfm.ksName, cfm.cfName);
            }
            else // has modifications in the nested ColumnFamilies, need to perform nested diff to determine what was really changed
            {
                String ksName = AsciiType.instance.getString(keyspace.key);

                Map<String, CFMetaData> oldCfDefs = new HashMap<String, CFMetaData>();
                for (CFMetaData cfm : Schema.instance.getKSMetaData(ksName).cfMetaData().values())
                    oldCfDefs.put(cfm.cfName, cfm);

                Map<String, CFMetaData> newCfDefs = KSMetaData.deserializeColumnFamilies(newRow);

                MapDifference<String, CFMetaData> cfDefDiff = Maps.difference(oldCfDefs, newCfDefs);

                for (CFMetaData cfDef : cfDefDiff.entriesOnlyOnRight().values())
                    addColumnFamily(cfDef);

                for (CFMetaData cfDef : cfDefDiff.entriesOnlyOnLeft().values())
                    dropColumnFamily(cfDef.ksName, cfDef.cfName);

                for (MapDifference.ValueDifference<CFMetaData> cfDef : cfDefDiff.entriesDiffering().values())
                    updateColumnFamily(cfDef.rightValue());
            }
        }
    }

    private static void addKeyspace(KSMetaData ksm)
    {
        assert Schema.instance.getKSMetaData(ksm.name) == null;
        Schema.instance.load(ksm);

        if (!StorageService.instance.isClientMode())
        {
            Table.open(ksm.name);
            MigrationManager.instance.notifyCreateKeyspace(ksm);
        }
    }

    private static void addColumnFamily(CFMetaData cfm)
    {
        assert Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName) == null;
        KSMetaData ksm = Schema.instance.getKSMetaData(cfm.ksName);
        ksm = KSMetaData.cloneWith(ksm, Iterables.concat(ksm.cfMetaData().values(), Collections.singleton(cfm)));

        Schema.instance.load(cfm);

        // make sure it's init-ed w/ the old definitions first,
        // since we're going to call initCf on the new one manually
        Table.open(cfm.ksName);

        Schema.instance.setTableDefinition(ksm);

        if (!StorageService.instance.isClientMode())
        {
            Table.open(ksm.name).initCf(cfm.cfId, cfm.cfName, true);
            MigrationManager.instance.notifyCreateColumnFamily(cfm);
        }
    }

    private static void updateKeyspace(KSMetaData newState)
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(newState.name);
        assert oldKsm != null;
        KSMetaData newKsm = KSMetaData.cloneWith(oldKsm.reloadAttributes(), oldKsm.cfMetaData().values());

        Schema.instance.setTableDefinition(newKsm);

        if (!StorageService.instance.isClientMode())
        {
            Table.open(newState.name).createReplicationStrategy(newKsm);
            MigrationManager.instance.notifyUpdateKeyspace(newKsm);
        }
    }

    private static void updateColumnFamily(CFMetaData newState)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(newState.ksName, newState.cfName);
        assert cfm != null;
        cfm.reload();

        if (!StorageService.instance.isClientMode())
        {
            Table table = Table.open(cfm.ksName);
            table.getColumnFamilyStore(cfm.cfName).reload();
            MigrationManager.instance.notifyUpdateColumnFamily(cfm);
        }
    }

    private static void dropKeyspace(String ksName)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
        String snapshotName = Table.getTimestampedSnapshotName(ksName);

        CompactionManager.instance.interruptCompactionFor(ksm.cfMetaData().values(), true);

        // remove all cfs from the table instance.
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            ColumnFamilyStore cfs = Table.open(ksm.name).getColumnFamilyStore(cfm.cfName);

            Schema.instance.purge(cfm);

            if (!StorageService.instance.isClientMode())
            {
                if (DatabaseDescriptor.isAutoSnapshot())
                    cfs.snapshot(snapshotName);
                Table.open(ksm.name).dropCf(cfm.cfId);
            }
        }

        // remove the table from the static instances.
        Table.clear(ksm.name);
        Schema.instance.clearTableDefinition(ksm);
        if (!StorageService.instance.isClientMode())
        {
            MigrationManager.instance.notifyDropKeyspace(ksm);
        }
    }

    private static void dropColumnFamily(String ksName, String cfName)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
        assert ksm != null;
        ColumnFamilyStore cfs = Table.open(ksName).getColumnFamilyStore(cfName);
        assert cfs != null;

        // reinitialize the table.
        CFMetaData cfm = ksm.cfMetaData().get(cfName);

        Schema.instance.purge(cfm);
        Schema.instance.setTableDefinition(makeNewKeyspaceDefinition(ksm, cfm));

        CompactionManager.instance.interruptCompactionFor(Arrays.asList(cfm), true);

        if (!StorageService.instance.isClientMode())
        {
            if (DatabaseDescriptor.isAutoSnapshot())
                cfs.snapshot(Table.getTimestampedSnapshotName(cfs.name));
            Table.open(ksm.name).dropCf(cfm.cfId);
            MigrationManager.instance.notifyDropColumnFamily(cfm);
        }
    }

    private static KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm, CFMetaData toExclude)
    {
        // clone ksm but do not include the new def
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(toExclude);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        return KSMetaData.cloneWith(ksm, newCfs);
    }

    private static void flushSchemaCFs()
    {
        flushSchemaCF(SystemTable.SCHEMA_KEYSPACES_CF);
        flushSchemaCF(SystemTable.SCHEMA_COLUMNFAMILIES_CF);
        flushSchemaCF(SystemTable.SCHEMA_COLUMNS_CF);
    }

    private static void flushSchemaCF(String cfName)
    {
        FBUtilities.waitOnFuture(SystemTable.schemaCFS(cfName).forceFlush());
    }
}
