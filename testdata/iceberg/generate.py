import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType


def gen_simple_cities(catalog):
    df = pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
            {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
            {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
            {"city": "Paris", "lat": 48.864716, "long": 2.349014},
        ],
    )
    schema = Schema(
        NestedField(1, "city", StringType(), required=False),
        NestedField(2, "lat", DoubleType(), required=False),
        NestedField(3, "long", DoubleType(), required=False),
    )
    tbl = catalog.create_table("default.cities", schema=schema)
    tbl.overwrite(df)


def main():
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": "sqlite:///:memory:",
            "warehouse": "file://wh",
        },
    )
    catalog.create_namespace("default")
    # gen_simple_cities(catalog)


if __name__ == "__main__":
    main()
