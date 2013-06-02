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

import java.io.IOError;
import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;

/**
 * @author smeet
 */
public class IdentityFilterOfRowCache implements FilterOfRowCache
{
    public static IdentityFilterOfRowCache instance = new IdentityFilterOfRowCache();

    @Override
    public QueryFilter queryFilterProvider(DecoratedKey key, String columnFamilyName)
    {
        return QueryFilter.getIdentityFilter(key, new QueryPath(columnFamilyName));
    }

    @Override
    public boolean updateRowCache(DecoratedKey key, ColumnFamily cachedRow, ColumnFamily columnFamily, int gcBefore)
    {
        cachedRow.addAll(columnFamily);
        return true;
    }

    @Override
    public ColumnFamily columnFamilyFilter(ColumnFamily cached, QueryFilter filter, int gcBefore)
    {
        // special case slicing the entire row:
        // we can skip the filter step entirely, and we can help out removeDeleted by re-caching the result
        // if any tombstones have aged out since last time.  (This means that the row cache will treat gcBefore as
        // max(gcBefore, all previous gcBefore), which is fine for correctness.)
        //
        // But, if the filter is asking for less columns than we have cached, we fall back to the slow path
        // since we have to copy out a subset.
        if (filter.filter instanceof SliceQueryFilter)
        {
            SliceQueryFilter sliceFilter = (SliceQueryFilter) filter.filter;
            if (sliceFilter.start.remaining() == 0 && sliceFilter.finish.remaining() == 0)
            {
                if (cached.isSuper() && filter.path.superColumnName != null)
                {
                    // subcolumns from named supercolumn
                    IColumn sc = cached.getColumn(filter.path.superColumnName);
                    if (sc == null || sliceFilter.count >= sc.getSubColumns().size())
                    {
                        ColumnFamily cf = cached.cloneMeShallow();
                        if (sc != null)
                            cf.addColumn(sc);
                        return ColumnFamilyStore.removeDeleted(cf, gcBefore);
                    }
                }
                else
                {
                    // top-level columns
                    if (sliceFilter.count >= cached.getEstimatedColumnCount())
                    {
                        return ColumnFamilyStore.removeDeleted(cached, gcBefore);
                    }
                }
            }
        }

        return collateAndRemoveDeleted(cached, filter, gcBefore);
    }

    @Override
    public Map<String, String> getParams()
    {
        return null;
    }

    public static ColumnFamily collateAndRemoveDeleted(ColumnFamily cached, QueryFilter filter, int gcBefore)
    {
        IColumnIterator ci = filter.getMemtableColumnIterator(cached, null, cached.getComparator());
        ColumnFamily cf;
        try
        {
            cf = ci.getColumnFamily().cloneMeShallow();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        filter.collectCollatedColumns(cf, ci, gcBefore);
        // TODO this is necessary because when we collate supercolumns together, we don't check
        // their subcolumns for relevance, so we need to do a second prune post facto here.
        return cf.isSuper() ? ColumnFamilyStore.removeDeleted(cf, gcBefore) : ColumnFamilyStore.removeDeletedCF(cf, gcBefore);
    }
}

