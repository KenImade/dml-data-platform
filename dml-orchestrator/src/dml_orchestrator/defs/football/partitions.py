from dagster import StaticPartitionsDefinition

gameweek_partitions = StaticPartitionsDefinition([f"GW{i}" for i in range(1, 39)])
