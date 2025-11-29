import io
import polars as pl


def dataframe_to_parquet_bytes(df: pl.DataFrame) -> bytes:
    """
    Converts a Polars DataFrame into Parquet bytes for storage or transport.
    """
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd")
    return buf.getvalue()
