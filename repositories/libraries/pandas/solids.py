from dagster import (
    solid,
    Field
)
import pandas as pd
import pendulum

@solid(config_schema={"read_csv_kwargs": Field(dict), "reindex_kwargs": dict})
def load_and_reindex_csv(context, file_path: str):
    read_csv_kwargs = context.solid_config["read_csv_kwargs"] 
    reindex_kwargs = context.solid_config["reindex_kwargs"] 

    # Rearrange columns
    # df = pd.read_csv(file_path, delimiter=delimiter, 
    #                  skiprows=header_lines,
    #                  names=original_header,
    #                  index_col=False)
    df = pd.read_csv(file_path, **read_csv_kwargs)
    df = df.reindex(**reindex_kwargs)
    return df


@solid(
    required_resource_keys={"timezone_config"},
)
def add_timestamp(context ,df):
    timezone = context.resources.timezone_config["timezone"]
    timestamp_captura = pd.to_datetime(pendulum.now(timezone).isoformat())
    df["timestamp_captura"] = timestamp_captura
    context.log.debug(", ".join(list(df.columns)))

    return df