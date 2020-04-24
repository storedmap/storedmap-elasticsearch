/*
 * Copyright 2019 fedd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.storedmap.elasticsearch;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author fedd
 */
public class MapUtility {

    public static List<Object> convertNumbersToDecimals(Iterable<Object> list) {
        ArrayList ret = new ArrayList();
        for (Object what : list) {
            what = convertNumbersToDecimals(what);
            ret.add(what);
        }
        return ret;
    }

    public static Object convertNumbersToDecimals(Object what) {
        if (what == null) {
            return null;
        } else if (what instanceof Map) {
            return convertNumbersToDecimals((Map<String, Object>) what);
        } else if (what instanceof Number) {
            return new BigDecimal(((Number) what).toString());
        } else if (what instanceof Iterable) {
            return convertNumbersToDecimals((Iterable) what);
            //} else if (what.getClass().isArray()) {
            //    return convertNumbersToDecimals(Arrays.asList(what));
        } else {
            return what;
        }
    }

    public static Map<String, Object> convertNumbersToDecimals(Map<String, Object> map) {
        map = new HashMap<>(map);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object what = entry.getValue();
            what = convertNumbersToDecimals(what);
            entry.setValue(what);
        }
        return map;
    }

}
