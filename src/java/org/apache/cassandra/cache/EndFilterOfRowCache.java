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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.FBUtilities;

public class EndFilterOfRowCache implements FilterOfRowCache
{
    public static final String ROW_CACHE_LIMIT = "limit";
    public static final String ROW_CACHE_REVERSED = "reversed";
    
    private final int limit;
    private final boolean reversed; // default false
    private final Map<String, String> filterParams;

    public EndFilterOfRowCache(Map<String, String> filterParams)
    {
        this.filterParams = filterParams;
        String rowCacheLimit = filterParams.get(ROW_CACHE_LIMIT);
        if (rowCacheLimit == null)
        {
            throw new IllegalArgumentException("row cache limit not set");
        }
        limit = Integer.parseInt(rowCacheLimit);
        String cacheReversed = filterParams.get(ROW_CACHE_REVERSED);
        reversed = (cacheReversed != null) && Boolean.parseBoolean(cacheReversed);
    }

    public QueryFilter queryFilterProvider(DecoratedKey key, String columnFamilyName)
    {
        return new QueryFilter(key, new QueryPath(columnFamilyName), 
                               new SliceQueryFilter(FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, 
                                                    reversed, limit));
    }

    public boolean updateRowCache(DecoratedKey key, ColumnFamily cachedRow, ColumnFamily columnsToAdd, int gcBefore)
    {
        AbstractType comparator = cachedRow.getComparator();
        ConcurrentSkipListMap<ByteBuffer, IColumn> cachedColumns = cachedRow.getColumnsMap();

        // TODO: the size count might hurt for larger rows like caching 10k friend ids
        if (reversed)
        {
            for (IColumn column : columnsToAdd.getColumnsMap().descendingMap().values())
            {
                if (cachedRow.getEstimatedColumnCount() < limit ||
                    comparator.compare(column.name(), cachedColumns.firstKey()) > 0)
                {
                    if (column.isMarkedForDelete())
                        return false;
                    
                    cachedRow.addColumn(column);
                }
            }
        }
        else
        {
            for (IColumn column : columnsToAdd.getColumnsMap().values())
            {
                if (cachedRow.getEstimatedColumnCount() < limit ||
                    comparator.compare(cachedColumns.lastKey(), column.name()) > 0)
                {
                    if (column.isMarkedForDelete())
                        return false;

                    cachedRow.addColumn(column);
                }
            }
        }
        
        cachedRow.delete(columnsToAdd);
        
        if (cachedRow.getEstimatedColumnCount() > limit) 
        {
            ColumnFamilyStore.removeDeleted(cachedRow, gcBefore);
            
            while (true)
            {
                ByteBuffer name = reversed ? cachedColumns.firstKey() : cachedColumns.lastKey();                
                if (cachedRow.getEstimatedColumnCount() <= limit)
                    break;
                
                cachedRow.remove(name);
            }
        }
        
        return true;
    }

    @Override
    public ColumnFamily columnFamilyFilter(ColumnFamily cachedRow, QueryFilter filter, int gcBefore) 
    {
        ConcurrentSkipListMap<ByteBuffer, IColumn> cachedMap = cachedRow.getColumnsMap();
        AbstractType comparator = cachedRow.getComparator();
        
        if (filter.filter instanceof SliceQueryFilter) {
            SliceQueryFilter sliceQueryFilter = (SliceQueryFilter) filter.filter;
            if (sliceQueryFilter.reversed != reversed)
                return null;
            
            // if the finish bound of the filter is not in the cache and the query limit is greater than the cache size we're out of luck
            if (sliceQueryFilter.count > limit && (!sliceQueryFilter.finish.hasRemaining() || 
                    (reversed && comparator.compare(sliceQueryFilter.finish, cachedMap.firstKey()) < 0)) ||
                    (!reversed && comparator.compare(sliceQueryFilter.finish, cachedMap.lastKey()) > 0))
                return null;
            
            if (!sliceQueryFilter.start.hasRemaining())
                return IdentityFilterOfRowCache.collateAndRemoveDeleted(cachedRow, filter, gcBefore);

                        
            if (reversed && comparator.compare(sliceQueryFilter.start, cachedMap.firstKey()) > 0)
            {
                ColumnFamily returnCF = IdentityFilterOfRowCache.collateAndRemoveDeleted(cachedRow, filter, gcBefore);
                if (sliceQueryFilter.finish.hasRemaining() && comparator.compare(sliceQueryFilter.finish, cachedMap.firstKey()) >= 0)
                {
                    return returnCF;
                }
                else
                {
                    return getLiveColumnCount(returnCF) >= ((SliceQueryFilter) filter.filter).count ? returnCF : null;
                }
            }

            if (!reversed && comparator.compare(sliceQueryFilter.start, cachedMap.lastKey()) < 0)
            {
                ColumnFamily returnCF = IdentityFilterOfRowCache.collateAndRemoveDeleted(cachedRow, filter, gcBefore);
                if (sliceQueryFilter.finish.hasRemaining() && comparator.compare(sliceQueryFilter.finish, cachedMap.lastKey()) <= 0)
                {
                    return returnCF;
                }
                else 
                {
                    return getLiveColumnCount(returnCF) >= ((SliceQueryFilter) filter.filter).count ? returnCF : null;
                }
            }
                
            return null;
            
            
        } else if (filter.filter instanceof NamesQueryFilter) {
            NamesQueryFilter namesQueryFilter = (NamesQueryFilter) filter.filter;
            SortedSet<ByteBuffer> columns = namesQueryFilter.columns;

            ByteBuffer firstKey = cachedMap.firstKey();
            ByteBuffer lastKey = cachedMap.lastKey();
            for (ByteBuffer column : columns) {
                if (reversed && comparator.compare(column, firstKey) < 0 ||
                    !reversed && comparator.compare(column, lastKey) > 0)
                    return null;
            }
            ColumnFamily returnCF = IdentityFilterOfRowCache.collateAndRemoveDeleted(cachedRow, filter, gcBefore);
            // if the cached map bounds have been changed theres a chance that we missed a column
            return (comparator.compare(firstKey, cachedMap.firstKey()) == 0 && comparator.compare(lastKey, cachedMap.lastKey()) == 0) ? returnCF : null;
        }
        
        return null;
    }

    @Override
    public Map<String, String> getParams()
    {
        return filterParams;
    }

    private int getLiveColumnCount(ColumnFamily returnCF)
    {
        int count = 0;
        for (IColumn column : returnCF.getSortedColumns())
        {
            if (column.isLive()) count++;
        }
        return count;
    }
}
