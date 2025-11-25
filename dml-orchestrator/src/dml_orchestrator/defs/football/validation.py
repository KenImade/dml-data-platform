import polars as pl
import io
from .models import Player


def validate_players_csv(csv_bytes: bytes) -> pl.DataFrame:
    df = pl.read_csv(io.BytesIO(csv_bytes))

    valid_rows = []
    for row in df.iter_rows(named=True):
        try:
            player = Player(**row)
            valid_rows.append(player.dict())
        except Exception as e:
            print(f"Skipping invalid rows: {row}, error: {e}")
    validated_df = pl.DataFrame(valid_rows)
    return validated_df
