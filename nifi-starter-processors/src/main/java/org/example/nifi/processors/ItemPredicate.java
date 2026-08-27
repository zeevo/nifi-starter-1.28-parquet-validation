/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.nifi.processors;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;

import java.io.Serializable;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.io.api.Binary;

/**
 * The same rules as {@link Item}, expressed as a Parquet predicate so the reader can apply them
 * instead of this code applying them to every decoded row.
 *
 * <p>The point is not to save the comparisons, it is what the predicate lets parquet-java skip.
 * With statistics filtering on, a row group whose {@code id} maximum is not above zero cannot
 * contain a passing row, so the whole group is skipped without decoding it. With column index
 * filtering on, the same reasoning applies at page granularity.
 *
 * <p>Keeping this in step with {@link Item} by hand is the obvious hazard, so
 * {@code PredicatePushdownTest} checks the two agree row by row on every fixture.
 */
final class ItemPredicate {

    private ItemPredicate() {
    }

    static FilterPredicate keepValidRows() {
        return and(
                // id present and positive. gt already excludes nulls.
                gt(longColumn(Item.ID), 0L),
                and(
                        // at least one of name and status present
                        or(notEq(binaryColumn(Item.NAME), null),
                           notEq(binaryColumn(Item.STATUS), null)),
                        and(
                                // name, when present, not blank
                                userDefined(binaryColumn(Item.NAME), NotBlank.class),
                                // status, when present, one of the allowed values
                                userDefined(binaryColumn(Item.STATUS), AllowedStatus.class))));
    }

    /**
     * Null passes: "name is blank" only rejects a name that is present and empty. A separate rule
     * already covers name and status both being null.
     */
    public static final class NotBlank extends UserDefinedPredicate<Binary> implements Serializable {

        @Override
        public boolean keep(final Binary value) {
            return value == null || !value.toStringUsingUTF8().trim().isEmpty();
        }

        /**
         * Blankness is not deducible from min/max, so no row group or page can be skipped on this
         * rule alone. Returning false everywhere means "cannot rule this block out".
         */
        @Override
        public boolean canDrop(final Statistics<Binary> statistics) {
            return false;
        }

        @Override
        public boolean inverseCanDrop(final Statistics<Binary> statistics) {
            return false;
        }
    }

    /** Null passes, for the same reason as above. */
    public static final class AllowedStatus extends UserDefinedPredicate<Binary> implements Serializable {

        @Override
        public boolean keep(final Binary value) {
            return value == null || Item.isAllowedStatus(value.toStringUsingUTF8());
        }

        @Override
        public boolean canDrop(final Statistics<Binary> statistics) {
            return false;
        }

        @Override
        public boolean inverseCanDrop(final Statistics<Binary> statistics) {
            return false;
        }
    }
}
