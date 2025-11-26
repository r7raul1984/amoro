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

package org.apache.amoro.spark.reader;

import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableList;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.TripleIterator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.Type;

import java.lang.reflect.Array;
import java.util.List;

public class ParquetValueReaders {
    private ParquetValueReaders() {}

    public abstract static class StructReader<T, I> implements ParquetValueReader<T> {
        private final ParquetValueReader<?>[] readers;
        private final TripleIterator<?> column;
        private final List<TripleIterator<?>> children;

        protected StructReader(List<Type> types, List<ParquetValueReader<?>> readers) {
            this(readers);
        }

        protected StructReader(List<ParquetValueReader<?>> readers) {
            this.readers =
                    (ParquetValueReader<?>[]) Array.newInstance(ParquetValueReader.class, readers.size());
            TripleIterator<?>[] columns =
                    (TripleIterator<?>[]) Array.newInstance(TripleIterator.class, readers.size());

            ImmutableList.Builder<TripleIterator<?>> columnsBuilder = ImmutableList.builder();
            for (int i = 0; i < readers.size(); i += 1) {
                ParquetValueReader<?> reader = readers.get(i);
                this.readers[i] = readers.get(i);
                columns[i] = reader.column();
                columnsBuilder.addAll(reader.columns());
            }

            this.children = columnsBuilder.build();
            this.column = firstNonNullColumn(children);
        }

        @Override
        public final void setPageSource(PageReadStore pageStore) {
            for (ParquetValueReader<?> reader : readers) {
                reader.setPageSource(pageStore);
            }
        }

        @Override
        public void setPageSource(PageReadStore pageStore, long rowPosition) {
            for (ParquetValueReader<?> reader : readers) {
                reader.setPageSource(pageStore);
            }
        }

        @Override
        public final TripleIterator<?> column() {
            return column;
        }

        @Override
        public final T read(T reuse) {
            I intermediate = newStructData(reuse);

            for (int i = 0; i < readers.length; i += 1) {
                set(intermediate, i, readers[i].read(get(intermediate, i)));
                // setters[i].set(intermediate, i, get(intermediate, i));
            }

            return buildStruct(intermediate);
        }

        @Override
        public List<TripleIterator<?>> columns() {
            return children;
        }

        @SuppressWarnings("unchecked")
        private <E> E get(I intermediate, int pos) {
            return (E) getField(intermediate, pos);
        }

        protected abstract I newStructData(T reuse);

        protected abstract Object getField(I intermediate, int pos);

        protected abstract T buildStruct(I struct);

        /**
         * Used to set a struct value by position.
         *
         * <p>To avoid boxing, override {@link #setInteger(Object, int, int)} and similar methods.
         *
         * @param struct a struct object created by {@link #newStructData(Object)}
         * @param pos the position in the struct to set
         * @param value the value to set
         */
        protected abstract void set(I struct, int pos, Object value);

        protected void setNull(I struct, int pos) {
            set(struct, pos, null);
        }

        protected void setBoolean(I struct, int pos, boolean value) {
            set(struct, pos, value);
        }

        protected void setInteger(I struct, int pos, int value) {
            set(struct, pos, value);
        }

        protected void setLong(I struct, int pos, long value) {
            set(struct, pos, value);
        }

        protected void setFloat(I struct, int pos, float value) {
            set(struct, pos, value);
        }

        protected void setDouble(I struct, int pos, double value) {
            set(struct, pos, value);
        }

        /**
         * Find a non-null column or return NULL_COLUMN if one is not available.
         *
         * @param columns a collection of triple iterator columns
         * @return the first non-null column in columns
         */
        private TripleIterator<?> firstNonNullColumn(List<TripleIterator<?>> columns) {
            for (TripleIterator<?> col : columns) {
                if (col != NullReader.NULL_COLUMN) {
                    return col;
                }
            }
            return NullReader.NULL_COLUMN;
        }
    }

    private static class NullReader<T> implements ParquetValueReader<T> {
        private static final ParquetValueReaders.NullReader<Void> INSTANCE =
                new ParquetValueReaders.NullReader<>();
        private static final ImmutableList<TripleIterator<?>> COLUMNS = ImmutableList.of();
        private static final TripleIterator<?> NULL_COLUMN =
                new TripleIterator<Object>() {
                    @Override
                    public int currentDefinitionLevel() {
                        return 0;
                    }

                    @Override
                    public int currentRepetitionLevel() {
                        return 0;
                    }

                    @Override
                    public <N> N nextNull() {
                        return null;
                    }

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }
                };

        private NullReader() {}

        @Override
        public T read(T reuse) {
            return null;
        }

        @Override
        public TripleIterator<?> column() {
            return NULL_COLUMN;
        }

        @Override
        public List<TripleIterator<?>> columns() {
            return COLUMNS;
        }

        @Override
        public void setPageSource(PageReadStore pageStore) {}
    }
}
