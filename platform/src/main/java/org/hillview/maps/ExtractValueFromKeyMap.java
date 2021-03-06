/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.maps;

import org.hillview.utils.Utilities;

import javax.annotation.Nullable;

/**
 * Creates a new string column by extracting data from another string column.
 * The column data has the shape ( k="value")*; this extracts the value
 * associated with a specified key.
 */
public class ExtractValueFromKeyMap extends CreateColumnMap {
    private final String key;

    public ExtractValueFromKeyMap(String key, String inputColumn, String newColumn,
                                  int insertionIndex) {
        super(inputColumn, newColumn, insertionIndex);
        this.key = key;
    }

    @Override
    @Nullable
    public String extract(@Nullable String s) {
        if (s == null)
            return null;
        if (s.startsWith("[") && s.endsWith("]"))
            s = s.substring(1, s.length() - 2);
        return Utilities.getKV(s, this.key);
    }
}
