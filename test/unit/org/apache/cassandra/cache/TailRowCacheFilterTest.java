/**
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

package org.apache.cassandra.cache;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.commons.lang.ArrayUtils.EMPTY_BYTE_ARRAY;

public class TailRowCacheFilterTest 
{
    private static final DecoratedKey ROW_KEY = getKey(); 

    @Test
    public void testCacheHit()
    {
        TailRowCacheFilter cacheFilter = createFilter(10, false);
        ColumnFamily cachedRow = fillColumnFamily(
                createColumnFamily(), 
                createColumns(1, 10, 1));

        // !reversed tests 
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, false, 10), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, true, 10), gcBefore()) == null;
        
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, array(10), false, 11), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, array(11), false, 11), gcBefore()) == null;
        
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, array(5), false, 11), gcBefore()) != null;
        
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(5), array(10), false, 11), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(0), array(10), false, 11), gcBefore()) != null;

        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, false, 11), gcBefore()) == null;

        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(8), EMPTY_BYTE_ARRAY, false, 3), gcBefore()) != null;        
        cachedRow.addTombstone(buffer(9), currentTimeSecs(), System.currentTimeMillis());
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(8), EMPTY_BYTE_ARRAY, false, 3), gcBefore()) == null;
        
        cacheFilter = createFilter(10, true);
        cachedRow = fillColumnFamily(
                createColumnFamily(), 
                createColumns(1, 10, 1));
        
        // reversed tests 
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, true, 10), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, false, 10), gcBefore()) == null;
        
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, array(10), true, 11), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, array(11), true, 11), gcBefore()) == null;
        
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, array(5), true, 11), gcBefore()) != null;
        
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(10), array(5), true, 11), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(11), array(5), true, 11), gcBefore()) != null;
                
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, true, 11), gcBefore()) == null;
        
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(3), EMPTY_BYTE_ARRAY, true, 3), gcBefore()) != null;
        cachedRow.addTombstone(buffer(2), currentTimeSecs(), System.currentTimeMillis());
        assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(3), EMPTY_BYTE_ARRAY, true, 3), gcBefore()) == null;
        
        
        cacheFilter = createFilter(10, false);
        cachedRow = fillColumnFamily(
                createColumnFamily(), 
                createColumns(1, 10, 1));
        
        // test some column querries (non reversed)
        assert cacheFilter.filterColumnFamily(cachedRow, createNameFilter(array(1), array(3)), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createNameFilter(array(0), array(3)), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createNameFilter(array(1), array(11)), gcBefore()) == null;

        cacheFilter = createFilter(10, true);
        cachedRow = fillColumnFamily(
                createColumnFamily(), 
                createColumns(1, 10, 1));
        
        // reversed tests 
        assert cacheFilter.filterColumnFamily(cachedRow, createNameFilter(array(1), array(3)), gcBefore()) != null;
        assert cacheFilter.filterColumnFamily(cachedRow, createNameFilter(array(0), array(3)), gcBefore()) == null;
        assert cacheFilter.filterColumnFamily(cachedRow, createNameFilter(array(1), array(11)), gcBefore()) != null;
    }
    
    @Test
    public void testApplyUpdate()
    {
        TailRowCacheFilter cacheFilter = createFilter(10, false);
        ColumnFamily cachedRow = fillColumnFamily(
                createColumnFamily(), 
                createColumns(0, 10, 2));
        
        
        // overfill by one
        cacheFilter.applyUpdate(ROW_KEY, cachedRow, fillColumnFamily(createColumnFamily(), createColumns(12, 20, 2)), gcBefore());
        
        assert cachedRow.getEstimatedColumnCount() == 10 : cachedRow.getEstimatedColumnCount();
        // 20 got dropped
        assert cachedRow.getColumnsMap().lastKey().get(0) == 18;
        
        cacheFilter.applyUpdate(ROW_KEY, cachedRow, fillColumnFamily(createColumnFamily(), createColumns(1, 10, 2)), gcBefore());

        assert cachedRow.getEstimatedColumnCount() == 10 : cachedRow.getEstimatedColumnCount();
        assert cachedRow.getColumnsMap().lastKey().get(0) == 9;
    }

    @Test
    // this is not a real test. just debug code to meassure performance impact of the applyUpdate method
    public void comparePerformance() throws InterruptedException
    {
        ColumnFamily cachedRow = fillColumnFamily(
                createColumnFamily(), 
                createColumns(0, 1000000, 2));
        final TailRowCacheFilter cacheFilter = createFilter(100000, false);
        long start = System.currentTimeMillis();
        for (int x = 0; x < 1000; x++)
        for (int i = 0; i < 9000; i++)
        {
            assert cacheFilter.filterColumnFamily(cachedRow, createSliceFilter(array(i * 10), EMPTY_BYTE_ARRAY, false, 10), gcBefore()) != null;
            cacheFilter.applyUpdate(ROW_KEY, cachedRow, fillColumnFamily(createColumnFamily(), createColumns(i*2+1, i*2+1, 1)), gcBefore());
        }
        long tailFilterTime = System.currentTimeMillis() - start;
        
        cachedRow = fillColumnFamily(
                createColumnFamily(), 
                createColumns(0, 1000000, 2));
        
        start = System.currentTimeMillis();
        for (int x = 0; x < 1000; x++)
        for (int i = 0; i < 9000; i++)
        {
            assert IdentityRowCacheFilter.instance.filterColumnFamily(cachedRow, createSliceFilter(array(i * 10), EMPTY_BYTE_ARRAY, false, 10), gcBefore()) != null;
            IdentityRowCacheFilter.instance.applyUpdate(ROW_KEY, cachedRow, fillColumnFamily(createColumnFamily(), createColumns(i*2+1, i*2+1, 1)), gcBefore());
        }
        long identityFilterTime = System.currentTimeMillis() - start;

        System.out.println("tailfiltertime: " + tailFilterTime);
        System.out.println("identityFilterTime: " + identityFilterTime);
        // keep alive for profiling ...
        Thread.sleep(100000);
    }

    private ColumnFamily createColumnFamily()
    {
        return new ColumnFamily(ColumnFamilyType.Standard, BytesType.instance, null, 1);
    }

    private QueryFilter createNameFilter(byte[] ... name) 
    {
        TreeSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(BytesType.instance);
        for (byte[] bytes : name)
            columns.add(ByteBuffer.wrap(bytes));

        return new QueryFilter(ROW_KEY, new QueryPath("test"), new NamesQueryFilter(columns));   
    }
    
    private QueryFilter createSliceFilter(byte[] start, byte[] finish, boolean reversed, int count) 
    {        
        return new QueryFilter(ROW_KEY, new QueryPath("Test"), new SliceQueryFilter(ByteBuffer.wrap(start), ByteBuffer.wrap(finish), reversed, count));
    }

    private ColumnFamily fillColumnFamily(ColumnFamily columnFamily, List<Column> columns)
    {
        for (Column column : columns) {
            columnFamily.addColumn(column);
        }
        return columnFamily;
    }
    
    private static List<Column> createColumns(int start, int finish, int stepping) 
    {
        ArrayList<Column> columns = new ArrayList<Column>();
        for (int i = start; i <= finish; i += stepping)
        {
            columns.add(new Column(ByteBuffer.wrap(array(i)), ByteBuffer.wrap(EMPTY_BYTE_ARRAY), System.currentTimeMillis()));
        }
        return columns;
    }
    
    private static ByteBuffer buffer(int i)
    {
        return ByteBuffer.wrap(array(i));
    }

    private static byte[] array(int i)
    {
        return array(new byte[]{(byte) i});
    }
    
    private static byte[] array(byte ... i)
    {
        return i;
    }

    private TailRowCacheFilter createFilter(int limit, boolean reversed) 
    {
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(TailRowCacheFilter.ROW_CACHE_LIMIT, String.valueOf(limit));
        params.put(TailRowCacheFilter.ROW_CACHE_REVERSED, String.valueOf(reversed));
        return new TailRowCacheFilter(params);
    }
    
    
    private static DecoratedKey getKey() {
        ByteBuffer key = ByteBuffer.wrap(new byte[]{1});
        return new DecoratedKey<BigIntegerToken>(getToken(key), key);
    }
    
    private static BigIntegerToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return new BigIntegerToken("-1");
        return new BigIntegerToken(FBUtilities.hashToBigInteger(key));
    }
    
    private int gcBefore()
    {
        return currentTimeSecs() - 864000;
    }

    private static int currentTimeSecs()
    {
        return (int) (System.currentTimeMillis() / 1000);
    }

}
