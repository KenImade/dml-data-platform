import polars as pl
import io
from typing import Type
from pydantic import BaseModel


def validate_csv(csv_bytes: bytes, schema: Type[BaseModel]) -> pl.DataFrame:
    df = pl.read_csv(io.BytesIO(csv_bytes))

    model_cols = set(schema.model_fields.keys())
    csv_cols = set(df.columns)

    missing = model_cols - csv_cols
    if missing:
        raise ValueError(f"Dataset is missing required columns: {sorted(missing)}")

    extra = csv_cols - model_cols
    if extra:
        raise ValueError(f"Dataset contains unexpected columns: {sorted(extra)}")

    # Build cast map: only simple types
    cast_map = {}
    for name, field in schema.model_fields.items():
        annotation = field.annotation
        if annotation in (int, float, str, bool):
            cast_map[name] = {
                int: pl.Int64,
                float: pl.Float64,
                str: pl.Utf8,
                bool: pl.Boolean,
            }[annotation]

    # Only cast columns that actually exist in the CSV
    df_casted = df.with_columns(
        [pl.col(col).cast(dtype, strict=True) for col, dtype in cast_map.items()]
    )

    return df_casted
