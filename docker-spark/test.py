import gc
import re
import string
import logging
from copy import copy, deepcopy
from ast import literal_eval
import pyspark
import underthesea
import numpy as np
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as f
from pyspark.sql.functions import col, udf, lit, greatest, monotonically_increasing_id, concat_ws
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, StructType, StructField, MapType, BooleanType, ArrayType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, MinHashLSH
from pyspark.ml.linalg import Vectors, VectorUDT

spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')\
    .getOrCreate()
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "earliest") \
    .load()
schema = StructType([
    StructField("address", StructType([
        StructField("district", StringType(), nullable=True),
        StructField("full_address", StringType(), nullable=True),
        StructField("province", StringType(), nullable=True),
        StructField("ward", StringType(), nullable=True),
    ]), nullable=True),
    StructField("contact_info", StructType([
        StructField("name", StringType(), nullable=True),
        StructField("phone", ArrayType(StringType()), nullable=True),
    ]), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("estate_type", StringType(), nullable=True),
    StructField("extra_infos", StructType([
        StructField("Chiều dài", StringType(), nullable=True),
        StructField("Chiều ngang", StringType(), nullable=True),
        StructField("Chính chủ", BooleanType(), nullable=True),
        StructField("Chổ để xe hơi", BooleanType(), nullable=True),
        StructField("Hướng", StringType(), nullable=True),
        StructField("Loại tin", StringType(), nullable=True),
        StructField("Lộ giới", StringType(), nullable=True),
        StructField("Nhà bếp", BooleanType(), nullable=True),
        StructField("Pháp lý", StringType(), nullable=True),
        StructField("Phòng ăn", BooleanType(), nullable=True),
        StructField("Sân thượng", BooleanType(), nullable=True),
        StructField("Số lầu", StringType(), nullable=True),
        StructField("Số phòng ngủ", StringType(), nullable=True),
        StructField("Số phòng ngủ :: string", StringType(), nullable=True),
        StructField("Số toilet :: string", StringType(), nullable=True),
        StructField("Tầng :: string", StringType(), nullable=True),
    ]), nullable=True),
    StructField("link", StringType(), nullable=True),
    StructField("post_date", StringType(), nullable=True),
    StructField("post_id", StringType(), nullable=True),
    StructField("price", StringType(), nullable=True),
    StructField("square", DoubleType(), nullable=True),
    StructField("title", StringType(), nullable=True),
])
json_string_df = streaming_df.selectExpr("CAST(value AS STRING) as json_string")

# Use the predefined schema with from_json
parsed_json_df = json_string_df.select(
    f.from_json("json_string", schema).alias("data")
)
final_df = parsed_json_df.select("data.*")

import os
import sys
import string
import re

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType, StructType, StructField, BooleanType, IntegerType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, MinHashLSH
from pyspark.ml.linalg import Vectors, VectorUDT
import underthesea
import numpy as np

# CASTING FUNCTIONS
def cast_to_string(value):
    try:
        return str(value)
    except (ValueError, TypeError):
        return None

def cast_to_boolean(value):
    try:
        return bool(value)
    except (ValueError, TypeError):
        return None

def cast_to_integer(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
    
def cast_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
    
# DUPLICATION PROCESSING FUNCTIONS
@udf(returnType=VectorUDT())
def append_non_zero_to_vector(vector, append_value=0.1):
    new_vector_dim = len(vector) + 1
    new_vector_indices = list(vector.indices) + [len(vector)]
    new_vector_values = list(vector.values) + [append_value]
    new_vector = Vectors.sparse(new_vector_dim,
                                new_vector_indices,
                                new_vector_values)
    return new_vector

def get_text_tfidf_vectors(df):
    df = df.withColumn("text", f.concat_ws(" ", f.col("title"), f.col("description"), f.col("address.full_address")))

    # Calculate TF-IDF vectors
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    df = tokenizer.transform(df)
    hashingTF = HashingTF(inputCol="tokens", outputCol="tf")
    df = hashingTF.transform(df)
    idf = IDF(inputCol="tf", outputCol="tfidf")
    idf_model = idf.fit(df)
    df = idf_model.transform(df)
    # Append non-zero value to vectors
    df = df = df.withColumn("text_vector", append_non_zero_to_vector(f.col("tfidf"), f.lit(0.1)))
    return df.drop("text", "tokens", "tf", "tfidf")

def get_duplicate_df_with_minhash(df, threshhold=0.5, num_hash_tables=3, dist_col="distCol"):
    # df must already have "id" column
    minhashLSH = MinHashLSH(inputCol="text_vector", outputCol="hashes", numHashTables=num_hash_tables)
    model = minhashLSH.fit(df)
    duplicate_df = model.approxSimilarityJoin(df.select("id", "text_vector"), df.select("id", "text_vector"), 0.8, distCol=dist_col) \
                         .filter("datasetA.id < datasetB.id")  # Avoid comparing a row to itself
    duplicate_df = duplicate_df.withColumn("id", f.col("datasetA.id")) \
                               .withColumn("duplicate_with_id", f.col("datasetB.id")) \
                               .select("id", "duplicate_with_id", dist_col)
    return duplicate_df

def remove_duplicated_rows(df, remove_ids):
    # df must already have "id" column
    remove_ids = remove_ids.select("id")
    result_df = df.join(remove_ids, df["id"] == remove_ids["id"], "leftanti")
    return result_df

# TEXT PROCESSING FUNCTIONS
def get_special_chars(df: pyspark.sql.dataframe.DataFrame):
    # get concatenated text
    concatenated_text = df.select(f.concat_ws(' ', f.col('title'), f.col('description')).alias('concatenated_text'))
    all_characters = concatenated_text.rdd.flatMap(lambda x: x[0])
    special_characters = all_characters.filter(lambda c: not c.isalnum() and not c.isspace() and not c in string.punctuation)
    return set(special_characters.collect())

def get_estate_types(df: pyspark.sql.dataframe.DataFrame):
    df = df.filter(df['estate_type'].isNotNull())
    all_estate_types = df.select('estate_type').rdd.map(lambda x: x[0])
    estate_types_set = set(all_estate_types.collect())
    return estate_types_set

@udf(returnType=StringType())
def remove_special_chars(input_string, special_chars_list, at_once=False):
    if not input_string:
        return None
    if at_once:
        special_chars_string = ''.join(special_chars_list)
        translator = str.maketrans('', '', special_chars_string)
        result = input_string.translate(translator)
    else:
        result = input_string
        for c in special_chars_list:
            result = result.replace(c, '')
    return result

@udf(returnType=StringType())
def remove_duplicate_punctuation_sequence(input_string):
    def remove_duplicate_sequence(text, target_char, max_length):
        pattern_1 = re.escape(target_char) + '{' + str(max_length) + ',}'
        pattern_2 = '(' + '\s' + re.escape(target_char) + ')' + '{' + str(max_length) + ',}'
        result = re.sub(pattern_2, target_char, re.sub(pattern_1, target_char, text))
        return result
    
    if not input_string:
        return None
    result = input_string
    for punc in string.punctuation:
        if punc == '\\':
            continue
        max_length = 3 if punc == '.' else 1
        reuslt = remove_duplicate_sequence(result, punc, max_length)
    return reuslt

@udf(returnType=StringType())
def normalize_estate_type(input_estate_type):
    if not input_estate_type:
        return None
    estate_type_prefix = ['Cho thuể', 'Mua bán', 'Căn hộ']
    estate_type_map = {
        'Biệt thự, liền k`ề': 'Biệt thự liền kề',
        'Nhà biệt thự liền kề': 'Biệt thự liền kề',
        'Nhà mặt phố': 'Nhà mặt tiền',
        'Phòng trọ, nhà trọ, nhà trọ': 'Phòng trọ, nhà trọ',
        'Phòng trọ': 'Phòng trọ, nhà trọ',
        'Trang trại, khu nghỉ dưỡng': 'Trang trại khu nghỉ dưỡng',
        'Kho nhà xưởng': 'Kho xưởng',
        'Kho, xưởng': 'Kho xưởng'
    }
    result = input_estate_type
    for prefix in estate_type_prefix:
        result = result.replace(prefix, '').strip().capitalize()
    for estate_type in estate_type_map.keys():
        if result == estate_type:
            result = estate_type_map[estate_type]
    return result

# NUMBERS PROCESSING FUNCTION
def get_lower_upper_bound(df, col_name, lower_percent=5, upper_percent=95, outlier_threshold=5):
    lower_percentile, upper_percentile = df.approxQuantile(col_name, [lower_percent/100, upper_percent/100], 0.01)
    quantile_range = upper_percentile - lower_percentile
    lower_bound = np.max([0, lower_percentile - outlier_threshold * quantile_range])
    upper_bound = upper_percentile + outlier_threshold * quantile_range
    return lower_bound, upper_bound

def get_detail_lower_upper_bound(df, col_name, lower_percent=5, upper_percent=95, outlier_threshold=5):
    quantiles_by_estate_type = (
        df.groupBy("estate_type")
        .agg(f.percentile_approx(col_name, [lower_percent/100, upper_percent/100], 100).alias("percentile_approx"))
    )
    quantiles_by_estate_type = quantiles_by_estate_type.withColumn("lower_percentile", f.col("percentile_approx").getItem(0)) \
                                                       .withColumn("upper_percentile", f.col("percentile_approx").getItem(1)) \
                                                       .withColumn("quantile_range", f.col("upper_percentile") - f.col("lower_percentile"))
    quantiles_by_estate_type = quantiles_by_estate_type.withColumn("lower_bound", f.greatest(f.col("lower_percentile") - outlier_threshold * f.col("quantile_range"), f.lit(0))) \
                                                       .withColumn("upper_bound", f.col("upper_percentile") + outlier_threshold * f.col("quantile_range"))
    
    return quantiles_by_estate_type.select("estate_type", "lower_bound", "upper_bound")

def filter_with_detail_bound(df, bound_df, join_col_name, filter_col_name):
    join_df = df.join(bound_df.alias("bound_df"), join_col_name, "inner")
    filtered_df = join_df.filter((join_df[filter_col_name] >= join_df["lower_bound"]) \
                                 & (join_df[filter_col_name] <= join_df["upper_bound"]))
    return filtered_df.drop("lower_bound", "upper_bound")

@udf(returnType=FloatType())
def price_normalize(price, square):
    if price is None:
        return None
    if isinstance(price, int) or isinstance(price, float):
        return price
    elif isinstance(price, str):
        if cast_to_float(price) is not None:
            return cast_to_float(price)
        if square is not None:
            price = underthesea.text_normalize(price)
            # Các trường hợp thực sự điền giá / m2
            if 'triệu/ m' in price or 'triệu / m' in price:
                price = float(price.split()[0]) * 1e6 * square
            # Các trường hợp điền nhầm giá sang giá / m2
            elif 'tỷ/ m' in price or 'tỷ / m' in price:
                price = float(price.split()[0]) * 1e9
            else:
                price = None
        elif square is None:
            price = None
    return price

# EXTRA INFOS PROCESSING FUNCTIONS
def get_extra_info_labels(df):
    extra_infos_df = df.select("extra_infos")
    extra_infos_labels = extra_infos_df.rdd.flatMap(lambda x: list(x[0].asDict().keys())).collect()
    return set(extra_infos_labels)
    
def normalize_text_field_in_dict(dict_obj):
    result_dict = dict_obj
    for key in result_dict.keys():
        if isinstance(result_dict[key], str):
            result_dict[key] = result_dict[key].replace(',', '.')
            new_val = ''
            for c in result_dict[key]:
                if c.isalpha() or c.isnumeric() or c == '.' or c == ' ':
                    new_val += c
            result_dict[key] = new_val
    return result_dict

@udf(returnType=StructType([
    StructField('Chiều dài', FloatType()),
    StructField('Chiều ngang', FloatType()),
    StructField('Chính chủ', BooleanType()),
    StructField('Chỗ để xe hơi', BooleanType()),
    StructField('Hướng', StringType()),
    StructField('Lộ giới', FloatType()),
    StructField('Nhà bếp', BooleanType()),
    StructField('Pháp lý', StringType()),
    StructField('Phòng ăn', BooleanType()),
    StructField('Sân thượng', BooleanType()),
    StructField('Số lầu', IntegerType()),
    StructField('Số phòng ngủ', IntegerType()),
    StructField('Số toilet', IntegerType()),
    StructField('Tầng', IntegerType()),
]))
def normalize_extra_infos_dict(input_extra_infos_row, old_keys, new_keys, remove_keys):
    old_keys = list(old_keys)
    new_keys = list(new_keys)
    remove_keys = list(remove_keys)
    assert len(old_keys) == len(new_keys)

    # Normalize dict keys
    extra_infos_dict = input_extra_infos_row.asDict()
    dict_nomalized_keys = {k: None for k in new_keys}

    for old_key, new_key in zip(old_keys, new_keys):
        if old_key in extra_infos_dict.keys():
            if new_key in dict_nomalized_keys.keys() and dict_nomalized_keys[new_key] is None \
                or new_key not in dict_nomalized_keys.keys():
                dict_nomalized_keys[new_key] = extra_infos_dict[old_key]
        else:
            dict_nomalized_keys[new_key] = None
    for key in remove_keys:
        if key in dict_nomalized_keys.keys():
            dict_nomalized_keys.pop(key)
    # Normalize dict values
    result_dict = normalize_text_field_in_dict(dict_nomalized_keys)
    result_dict['Chiều dài'] = cast_to_float(dict_nomalized_keys['Chiều dài'].replace('m', '')) if result_dict['Chiều dài'] is not None else None
    result_dict['Chiều ngang'] = cast_to_float(dict_nomalized_keys['Chiều ngang'].replace('m', '')) if result_dict['Chiều ngang'] is not None else None
    result_dict['Chính chủ'] = cast_to_boolean(dict_nomalized_keys['Chính chủ']) if result_dict['Chính chủ'] is not None else None
    result_dict['Chỗ để xe hơi'] = cast_to_boolean(dict_nomalized_keys['Chỗ để xe hơi']) if result_dict['Chỗ để xe hơi'] is not None else None
    result_dict['Hướng'] = cast_to_string(dict_nomalized_keys['Hướng']) if result_dict['Hướng'] is not None else None
    result_dict['Lộ giới'] = cast_to_float(dict_nomalized_keys['Lộ giới'].replace('m', '')) if result_dict['Lộ giới'] is not None else None
    result_dict['Nhà bếp'] = cast_to_boolean(dict_nomalized_keys['Nhà bếp']) if result_dict['Nhà bếp'] is not None else None
    result_dict['Pháp lý'] = cast_to_string(dict_nomalized_keys['Pháp lý']) if result_dict['Pháp lý'] is not None else None
    result_dict['Phòng ăn'] = cast_to_boolean(dict_nomalized_keys['Phòng ăn']) if result_dict['Phòng ăn'] is not None else None
    result_dict['Sân thượng'] = cast_to_boolean(dict_nomalized_keys['Sân thượng']) if result_dict['Sân thượng'] is not None else None
    result_dict['Số lầu'] = cast_to_integer(dict_nomalized_keys['Số lầu']) if result_dict['Số lầu'] is not None else None
    result_dict['Số phòng ngủ'] = cast_to_integer(dict_nomalized_keys['Số phòng ngủ']) if result_dict['Số phòng ngủ'] is not None else None
    result_dict['Số toilet'] = cast_to_integer(dict_nomalized_keys['Số toilet']) if result_dict['Số toilet'] is not None else None
    result_dict['Tầng'] = cast_to_integer(dict_nomalized_keys['Tầng']) if result_dict['Tầng'] is not None else None
    return result_dict

special_chars_list = ['→', '\u202a', '\uf0d8', '✤', '\u200c', 'ۣ', '🅖', '–', '₋', '●', '¬', '̶', '▬', '≈', '🫵', '◇', '▷', '🪷', '◊', '‐', '🫴', '\uf05b', '⦁', '️', '㎡', '🫰', '′', '✥', '✧', '♤', '🫶', 'ۜ', '❃', '̀', '֍', '\u2060', '\u206e', '‘', '❈', '🅣', '🅘', '℅', '\ufeff', '″', '\u200b', '♚', '̣', '₫', '\uf06e', '✩', '🅨', '’', '\xad', '★', '±', '\U0001fae8', '︎', '\uf0f0', '∙', '♛', '̉', '̛', '❆', '✜', '÷', '♜', '·', '❖', '】', '❁', '🫱', '・', '€', '☛', '“', '■', '\uf046', '￼', '�', '\u200d', '🫠', '\uf0e8', '⁃', '≥', '～', '➣', '́', '🪩', '̃', '\uf02b', '᪥', '🪺', '♧', '❂', '。', '♡', '，', '🪸', '：', '¥', '❝', '̂', '\U0001fa77', '\uf0a7', 'ৣ', '⚘', '➢', '⇔', '、', '－', '✆', '🫣', '⛫', '►', '̆', '✎', '❯', '《', '\uf076', '❮', '❀', '̵', '🥹', '❉', '̷', '\uf028', '✽', '«', '⇒', '➤', '\uf0e0', '\U0001faad', '♙', '\uf0fc', '【', '➥', '¤', '＆', '🛇', '\x7f', '）', '—', '”', '❞', '》', '☆', '×', '✞', '✿', '≤', '🅐', '√', '°', '✓', '¡', '…', '•', '»', '❊', '➦', '\u06dd', '\uf06c', '¸']
final_df = final_df.withColumn("title", remove_special_chars("title", f.lit(special_chars_list)))
final_df = final_df.withColumn("description", remove_special_chars("description", f.lit(special_chars_list)))
final_df = final_df.withColumn("title", remove_duplicate_punctuation_sequence("title"))
final_df = final_df.withColumn("description", remove_duplicate_punctuation_sequence("description"))
final_df = final_df.withColumn("estate_type", normalize_estate_type("estate_type"))
print("Text processed.")

final_df = final_df.withColumn("price/square", f.col("price")/f.col("square"))
final_df = final_df.withColumn("price", price_normalize("price", "square"))
print("Numbers processed.")

old_keys = ['Chiều dài', 'Chiều ngang', 'Chính chủ', 'Chổ để xe hơi', 'Hướng', 'Loại tin', 'Lộ giới', 'Nhà bếp', 'Pháp lý', 'Phòng ăn', 'Sân thượng', 'Số lầu', 'Số phòng ngủ', 'Số phòng ngủ :', 'Số toilet :', 'Tầng :']
new_keys = ['Chiều dài', 'Chiều ngang', 'Chính chủ', 'Chỗ để xe hơi', 'Hướng', 'remove', 'Lộ giới', 'Nhà bếp', 'Pháp lý', 'Phòng ăn', 'Sân thượng', 'Số lầu', 'Số phòng ngủ', 'Số phòng ngủ', 'Số toilet', 'Tầng']
remove_keys = ['remove']
final_df = final_df.withColumn("extra_infos", normalize_extra_infos_dict("extra_infos", f.lit(old_keys), f.lit(new_keys), f.lit(remove_keys)))
print("Extra infos processed.")

query = final_df.writeStream \
    .outputMode("append")  .format("console") .start()
query.awaitTermination()
