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

package org.apache.cassandra.rocksdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.rocksdb.encoding.RowKeyEncoder;
import org.apache.cassandra.rocksdb.encoding.orderly.Bytes;
import org.apache.cassandra.rocksdb.encoding.value.RowValueEncoder;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.rocksdb.RocksDBException;


public class RocksDBPartition implements Partition
{
    private final RocksDBCF db;
    private final DecoratedKey partitionKey;
    private final CFMetaData metadata;

    public RocksDBPartition(RocksDBCF db, DecoratedKey partitionKey, CFMetaData metadata)
    {
        this.db = db;
        this.partitionKey = partitionKey;
        this.metadata = metadata;
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return DeletionTime.LIVE;
    }

    public PartitionColumns columns()
    {
        return metadata.partitionColumns();
    }

    public EncodingStats stats()
    {
        return EncodingStats.NO_STATS;
    }

    public boolean isEmpty()
    {
        throw new NotImplementedException();
    }

    public Row getRow(Clustering clustering)
    {
        return getRow(clustering, ColumnFilter.all(metadata));
    }

    // Our implementation of SearchIterator looks a bit weird since given clustering info we don't
    // really need "search" like BTreePartition does. To us it is just a simple point query to the
    // underlying RocksDB.
    public SearchIterator<Clustering, Row> searchIterator(ColumnFilter columnFilter, boolean reversed)
    {
        return new SearchIterator<Clustering, Row>()
        {

            public boolean hasNext()
            {
                // always return true is fine since there are always limited amount of clustering
                // can be used to get 'next'
                return true;
            }

            public Row next(Clustering clustering)
            {
                if (clustering == Clustering.STATIC_CLUSTERING)
                {
                    return null;
                }
                return getRow(clustering, columnFilter);
            }
        };
    }

    private Row getRow(Clustering clustering, ColumnFilter columnFilter)
    {
        try
        {
            byte[] key = RowKeyEncoder.encode(partitionKey, clustering, metadata);
            byte[] values = db.get(key);

            return makeRow(values, columnFilter, clustering);
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        throw new NotImplementedException();
    }


    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
    {
        if(slices.size() == 1) {
            return sliceIterator(slices.get(0), columns, reversed
                    ? PartitionIterOrder.REVERSED
                    : PartitionIterOrder.NORMAL);
        }

        return new SlicesIterator(slices, columns, reversed);
    }

    private class SlicesIterator extends AbstractUnfilteredRowIterator {
        private final Iterator<Slice> slices;
        private final PartitionIterOrder iterOrder;
        private final ColumnFilter columnFilter;

        private Iterator<Unfiltered> currentSlice;

        private SlicesIterator(Slices slices, ColumnFilter columnFilter, boolean isReverseOrder) {
            super(RocksDBPartition.this.metadata, RocksDBPartition.this.partitionKey, DeletionTime.LIVE,
                    RocksDBPartition.this.metadata.partitionColumns(), null, isReverseOrder, EncodingStats.NO_STATS);
            this.slices = slices.iterator();
            this.iterOrder = isReverseOrder ? PartitionIterOrder.REVERSED : PartitionIterOrder.NORMAL;
            this.columnFilter = columnFilter;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if(slices.hasNext()) {
                        currentSlice = sliceIterator(slices.next(), columnFilter, iterOrder);
                    }
                    else {
                        return endOfData();
                    }
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }
    }


    private UnfilteredRowIterator sliceIterator(Slice slice, ColumnFilter columnFilter, PartitionIterOrder iterOrder)
    {
        byte[] partitionKeyBytes = RowKeyEncoder.encode(partitionKey, metadata);

        RocksDBIteratorAdapter rocksIterator = db.newIterator();

        byte[] minKey = slice.start() == Slice.Bound.BOTTOM ? null :
                             RowKeyEncoder.encode(partitionKey, slice.start().clustering(), metadata);

        byte[] maxKey = slice.end() == Slice.Bound.TOP ? null :
                             RowKeyEncoder.encode(partitionKey, slice.end().clustering(), metadata);

        iterOrder.seekToStart(rocksIterator, partitionKeyBytes, minKey, maxKey,
                              slice.start().isExclusive(), slice.end().isExclusive());


        return new AbstractUnfilteredRowIterator(metadata, partitionKey, DeletionTime.LIVE, metadata.partitionColumns(),
                                                 null, iterOrder == PartitionIterOrder.REVERSED, EncodingStats.NO_STATS)
        {
            public void close()
            {
                super.close();
                rocksIterator.close();
            }

            protected Unfiltered computeNext()
            {
                Unfiltered row = null;
                // keep moving rocksdb iterator forward until we get a row not been deleted
                // or we reach the end of the slice boundary
                while (row == null)
                {
                    if (!rocksIterator.isValid())
                    {
                        return endOfData();
                    }

                    byte[] key = rocksIterator.key();

                    if (!Bytes.startsWith(key, partitionKeyBytes) ||
                        exceedLowerBound(key, minKey, slice.start().isInclusive()) ||
                        exceedUpperBound(key, maxKey, slice.end().isInclusive()))
                    {
                        return endOfData();
                    }


                    byte[] value = rocksIterator.value();
                    row = makeRow(key, value, columnFilter);
                    iterOrder.moveForward(rocksIterator);
                }
                return row;
            }
        };
    }

    private boolean exceedLowerBound(byte[] key, byte[] lowerKey, boolean inclusive)
    {
        if (lowerKey == null)
        {
            return false;
        }

        if (inclusive)
        {
            return FBUtilities.compareUnsigned(key, lowerKey) < 0;
        }
        else
        {
            return FBUtilities.compareUnsigned(key, lowerKey) <= 0;
        }
    }

    private boolean exceedUpperBound(byte[] key, byte[] upperKey, boolean inclusive)
    {
        if (upperKey == null)
        {
            return false;
        }

        if (inclusive)
        {
            return FBUtilities.compareUnsigned(key, upperKey) > 0;
        }
        else
        {
            return FBUtilities.compareUnsigned(key, upperKey) >= 0;
        }
    }


    private Row makeRow(byte[] key, byte[] value, ColumnFilter columnFilter)
    {
        ByteBuffer[] decoded = RowKeyEncoder.decode(key, metadata);
        ByteBuffer[] clusteringKeys = Arrays.copyOfRange(decoded, metadata.partitionKeyColumns().size(), decoded.length);
        return makeRow(value, columnFilter, new BufferClustering(clusteringKeys));
    }

    private Row makeRow(byte[] value, ColumnFilter columnFilter, Clustering clustering)
    {
        if (value == null || value.length == 0)
        {
            return null;
        }

        List<ColumnData> dataBuffer = new ArrayList<>();

        RowValueEncoder.decode(metadata(), columnFilter, ByteBuffer.wrap(value), dataBuffer);

        if (dataBuffer.isEmpty())
        {
            //TODO: we need return tombstone instead of skipping the row for the case like quorum read
            return null;
        }

        return BTreeRow.create(clustering,
                               LivenessInfo.EMPTY,
                               Row.Deletion.regular(DeletionTime.LIVE),
                               BTree.build(dataBuffer, UpdateFunction.<ColumnData>noOp()));
    }
}
