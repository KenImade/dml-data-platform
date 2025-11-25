import polars as pl
import io
from typing import Type
from pydantic import BaseModel


def validate_csv(csv_bytes: bytes, schema: Type[BaseModel]) -> pl.DataFrame:
    """Validates CSV schema and returns a DataFrame"""
    df = pl.read_csv(io.BytesIO(csv_bytes))

    valid_rows = []
    for row in df.iter_rows(named=True):
        try:
            model = schema(**row)
            valid_rows.append(model.model_dump())
        except Exception as e:
            print(f"Skippind invalid rows: {row}, error: {e}")
    validated_df = pl.DataFrame(valid_rows)
    return validated_df
