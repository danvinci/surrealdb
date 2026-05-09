"""
Cross-SDK interop fixture: writes a known set of records to SurrealDB
using surrealdb-py. The Julia interop test reads them back and asserts
byte-equal field values.

Catches encoding drift between SDKs: if Python writes a datetime, integer,
or RecordID that Julia can't faithfully decode, the assertion fails and we
get a concrete signal about which type bridge is broken.

Run:
    pip install surrealdb
    python test/interop/write_test_data.py
"""
import datetime
import os
import sys
from surrealdb import Surreal

URL = os.environ.get("SURREALDB_URL", "ws://localhost:8000/rpc")
NS = os.environ.get("SURREALDB_NS", "test")
DB = os.environ.get("SURREALDB_DB", "test")


def main():
    db = Surreal(URL)
    db.signin({"username": "root", "password": "root"})
    db.use(NS, DB)

    # Define schema-less interop table.
    db.query("REMOVE TABLE IF EXISTS interop")
    db.query("DEFINE TABLE interop")

    fixtures = [
        ("interop:int_pos", {
            "kind": "int_positive",
            "value": 12345,
        }),
        ("interop:int_neg", {
            "kind": "int_negative",
            "value": -67890,
        }),
        ("interop:float_simple", {
            "kind": "float_simple",
            "value": 3.14159,
        }),
        ("interop:string_ascii", {
            "kind": "string_ascii",
            "value": "hello world",
        }),
        ("interop:string_unicode", {
            "kind": "string_unicode",
            "value": "αβγ ✓ 中文 🦀",
        }),
        ("interop:bool_true", {
            "kind": "bool_true",
            "value": True,
        }),
        ("interop:bool_false", {
            "kind": "bool_false",
            "value": False,
        }),
        ("interop:null", {
            "kind": "null_value",
            "value": None,
        }),
        ("interop:array_int", {
            "kind": "array_int",
            "value": [1, 2, 3, 4, 5],
        }),
        ("interop:array_mixed", {
            "kind": "array_mixed",
            "value": [1, "two", 3.0, True, None],
        }),
        ("interop:nested_object", {
            "kind": "nested_object",
            "value": {
                "outer": {
                    "inner": [10, 20, {"deep": "leaf"}],
                    "ts": datetime.datetime(2024, 1, 15, 12, 30, 45,
                                            tzinfo=datetime.timezone.utc),
                },
            },
        }),
    ]

    for record_id, data in fixtures:
        db.create(record_id, data)
        print(f"  wrote {record_id}", file=sys.stderr)

    print(f"wrote {len(fixtures)} interop fixtures", file=sys.stderr)


if __name__ == "__main__":
    main()
