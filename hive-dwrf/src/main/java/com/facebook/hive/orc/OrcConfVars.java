/*
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

package com.facebook.hive.orc;

import org.apache.hadoop.conf.Configuration;

public class OrcConfVars
{
    private static final String MERGE_BLOCK_LEVEL = "hive.merge.orc.block.level";
    private static final String COMPRESSION = "hive.exec.orc.compress";
    private static final String COMPRESS_ZLIB_LEVEL = "hive.exec.orc.compress.zlib.level";
    private static final String COMPRESS_SIZE = "hive.exec.orc.compress.size";
    private static final String STRIPE_SIZE = "hive.exec.orc.stripe.size";
    private static final String ROW_INDEX_STRIDE = "hive.exec.orc.row.index.stride";
    private static final String CREATE_INDEX = "hive.exec.orc.create.index";
    private static final String DICTIONARY_KEY_NUMERIC_SIZE_THRESHOLD = "hive.exec.orc.dictionary.key.numeric.size.threshold";
    private static final String DICTIONARY_KEY_STRING_SIZE_THRESHOLD = "hive.exec.orc.dictionary.key.string.size.threshold";
    private static final String DICTIONARY_KEY_SORTED = "hive.exec.orc.dictionary.key.sorted";
    private static final String ENTROPY_KEY_STRING_SIZE_THRESHOLD = "hive.exec.orc.entropy.key.string.size.threshold";
    private static final String ENTROPY_STRING_MIN_SAMPLES = "hive.exec.orc.entropy.string.min.samples";
    private static final String ENTROPY_STRING_DICT_SAMPLE_FRACTION = "hive.exec.orc.entropy.string.dict.sample.fraction";
    private static final String ENTROPY_STRING_THRESHOLD = "hive.exec.orc.entropy.string.threshold";
    private static final String ENCODING_INTERVAL = "hive.exec.orc.encoding.interval";
    private static final String USE_VINTS = "hive.exec.orc.use.vints";
    private static final String MEMORY_POOL = "hive.exec.orc.memory.pool";
    private static final String ROW_BUFFER_SIZE = "hive.exec.orc.row.buffer.size";

    public static boolean getMergeBlockLevel(Configuration conf)
    {
        return conf.getBoolean(MERGE_BLOCK_LEVEL, true);
    }

    public static void setMergeBlockLevel(Configuration conf, boolean value)
    {
        conf.setBoolean(MERGE_BLOCK_LEVEL, value);
    }

    public static String getCompression(Configuration conf)
    {
        return conf.get(COMPRESSION, "ZLIB");
    }

    public static void setCompression(Configuration conf, String value)
    {
        conf.set(COMPRESSION, value);
    }

    public static int getCompressZlibLevel(Configuration conf)
    {
        return conf.getInt(COMPRESS_ZLIB_LEVEL, -1);
    }

    public static void setCompressZlibLevel(Configuration conf, int value)
    {
        conf.setInt(COMPRESS_ZLIB_LEVEL, value);
    }

    public static int getCompressSize(Configuration conf)
    {
        return conf.getInt(COMPRESS_SIZE, 262144);
    }

    public static void setCompressSize(Configuration conf, int value)
    {
        conf.setInt(COMPRESS_SIZE, value);
    }

    public static long getStripeSize(Configuration conf)
    {
        return conf.getLong(STRIPE_SIZE, 268435456L);
    }

    public static void setStripeSize(Configuration conf, long value)
    {
        conf.setLong(STRIPE_SIZE, value);
    }

    public static int getRowIndexStride(Configuration conf)
    {
        return conf.getInt(ROW_INDEX_STRIDE, 10000);
    }

    public static void setMergeBlockLevel(Configuration conf, int value)
    {
        conf.setInt(ROW_INDEX_STRIDE, value);
    }

    public static boolean getCreateIndex(Configuration conf)
    {
        return conf.getBoolean(CREATE_INDEX, true);
    }

    public static void setCreateIndex(Configuration conf, boolean value)
    {
        conf.setBoolean(CREATE_INDEX, value);
    }

    public static float getDictionaryKeyNumericSizeThreshold(Configuration conf)
    {
        return conf.getFloat(DICTIONARY_KEY_NUMERIC_SIZE_THRESHOLD, 0.7f);
    }

    public static void setDictionaryKeyNumericSizeThreshold(Configuration conf, float value)
    {
        conf.setFloat(DICTIONARY_KEY_NUMERIC_SIZE_THRESHOLD, value);
    }

    public static float getDictionaryKeyStringSizeThreshold(Configuration conf)
    {
        return conf.getFloat(DICTIONARY_KEY_STRING_SIZE_THRESHOLD, 0.8f);
    }

    public static void setDictionaryKeyStringSizeThreshold(Configuration conf, float value)
    {
        conf.setFloat(DICTIONARY_KEY_STRING_SIZE_THRESHOLD, value);
    }

    public static boolean getDictionaryKeySorted(Configuration conf)
    {
        return conf.getBoolean(DICTIONARY_KEY_SORTED, true);
    }

    public static void setDictionaryKeySorted(Configuration conf, boolean value)
    {
        conf.setBoolean(DICTIONARY_KEY_SORTED, value);
    }

    public static float getEntropyKeyStringSizeThreshold(Configuration conf)
    {
        return conf.getFloat(ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.9f);
    }

    public static void setEntropyKeyStringSizeThreshold(Configuration conf, float value)
    {
        conf.setFloat(ENTROPY_KEY_STRING_SIZE_THRESHOLD, value);
    }

    public static int getEntropyStringMinSamples(Configuration conf)
    {
        return conf.getInt(ENTROPY_STRING_MIN_SAMPLES, 100);
    }

    public static void setEntropyStringMinSamples(Configuration conf, int value)
    {
        conf.setInt(ENTROPY_STRING_MIN_SAMPLES, value);
    }

    public static float getEntropyStringDictSampleFraction(Configuration conf)
    {
        return conf.getFloat(ENTROPY_STRING_DICT_SAMPLE_FRACTION, 0.001f);
    }

    public static void setEntropyStringDictSampleFraction(Configuration conf, float value)
    {
        conf.setFloat(ENTROPY_STRING_DICT_SAMPLE_FRACTION, value);
    }

    public static int getEntropyStringThreshold(Configuration conf)
    {
        return conf.getInt(ENTROPY_STRING_THRESHOLD, 20);
    }

    public static void setEntropyStringThreshold(Configuration conf, int value)
    {
        conf.setInt(ENTROPY_STRING_THRESHOLD, value);
    }

    public static int getEncodingInterval(Configuration conf)
    {
        return conf.getInt(ENCODING_INTERVAL, 30);
    }

    public static void setEncodingInterval(Configuration conf, int value)
    {
        conf.setInt(ENCODING_INTERVAL, value);
    }

    public static boolean getUseVints(Configuration conf)
    {
        return conf.getBoolean(USE_VINTS, true);
    }

    public static void setUseVints(Configuration conf, boolean value)
    {
        conf.setBoolean(USE_VINTS, value);
    }

    // Maximum fraction of heap that can be used by ORC file writers
    public static float getMemoryPool(Configuration conf)
    {
        return conf.getFloat(MEMORY_POOL, 0.5f); // 50%
    }

    public static void setMemoryPool(Configuration conf, float value)
    {
        conf.setFloat(MEMORY_POOL, value);
    }

    public static int getRowBufferSize(Configuration conf)
    {
        return conf.getInt(ROW_BUFFER_SIZE, 100);
    }

    public static void setRowBufferSize(Configuration conf, int value)
    {
        conf.setInt(ROW_BUFFER_SIZE, value);
    }
}
