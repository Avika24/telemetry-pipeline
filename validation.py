import polars as pl

def validate_batch(df: pl.DataFrame) -> pl.DataFrame:
    is_valid = (
        (pl.col("temperature").is_not_null()) & (pl.col("temperature").is_finite()) & (pl.col("temperature").is_between(-20, 60))
    ) & (
        (pl.col("humidity").is_not_null()) & (pl.col("humidity").is_finite()) & (pl.col("humidity").is_between(0, 100))
    ) & (
        (pl.col("cpu_usage").is_not_null()) & (pl.col("cpu_usage").is_finite()) & (pl.col("cpu_usage").is_between(0, 100))
    ) & (
        (pl.col("device_id").is_not_null()) & (pl.col("device_id").str.strip_chars().str.len_chars() > 0)
    )

    failure_reason = (
        pl.when(pl.col("temperature").is_not_null()).then(pl.lit("")).otherwise(pl.lit("Null Temp;"))
        + pl.when(pl.col("temperature").is_finite()).then(pl.lit("")).otherwise(pl.lit("Non-Finite Temp;"))
        + pl.when(pl.col("temperature").is_between(-20, 60)).then(pl.lit("")).otherwise(pl.lit("Temp Breach;"))
        + pl.when(pl.col("humidity").is_not_null()).then(pl.lit("")).otherwise(pl.lit("Null Humidity;"))
        + pl.when(pl.col("humidity").is_finite()).then(pl.lit("")).otherwise(pl.lit("Non-Finite Humidity;"))
        + pl.when(pl.col("humidity").is_between(0, 100)).then(pl.lit("")).otherwise(pl.lit("Humidity Breach;"))
        + pl.when(pl.col("cpu_usage").is_not_null()).then(pl.lit("")).otherwise(pl.lit("Null CPU;"))
        + pl.when(pl.col("cpu_usage").is_finite()).then(pl.lit("")).otherwise(pl.lit("Non-Finite CPU;"))
        + pl.when(pl.col("cpu_usage").is_between(0, 100)).then(pl.lit("")).otherwise(pl.lit("CPU Breach;"))
        + pl.when(pl.col("device_id").is_not_null()).then(pl.lit("")).otherwise(pl.lit("Null device_id;"))
        + pl.when(pl.col("device_id").str.strip_chars().str.len_chars() > 0).then(pl.lit("")).otherwise(pl.lit("Empty device_id;"))
    )

    df = df.with_columns(
        is_valid.alias("is_valid"),
        failure_reason.alias("failure_reason"),
    )

    return df

def split_batch(validated_df: pl.DataFrame):
     clean_df = validated_df.filter(pl.col("is_valid"))
     rejected_df = validated_df.filter(~pl.col("is_valid"))
     return clean_df, rejected_df
    