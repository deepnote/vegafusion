"""Python counterpart to test_plan_serialization.rs.

Deserializes proto bytes produced by the Rust test and rewrites the ViewTable
placeholder pattern back into a real registered table. This verifies that
plans serialized in Rust can be consumed in Python using only the default
datafusion-proto codec (no custom LogicalExtensionCodec).

Run the Rust test first to generate plan_bytes.bin:
    cargo test -p vegafusion-runtime --test test_plan_serialization
"""

from pathlib import Path

import pyarrow as pa
from datafusion import LogicalPlan, SessionContext


def rewrite_view_to_empty_table(ctx: SessionContext, plan: LogicalPlan) -> LogicalPlan:
    """Detect the ViewTable serialization pattern and replace it with a real table.

    The Rust side rewrites EmptyTable scans into:
        SubqueryAlias(<name>)
          Projection(CAST(NULL AS <type>) AS <col>, ...)
            EmptyRelation

    We extract the table name and schema from this structure. To get the Arrow
    schema without manually parsing expr types, we create a DataFrame from the
    projection plan — it produces 0 rows but carries the correct column types.
    """
    variant = plan.to_variant()
    if type(variant).__name__ != "SubqueryAlias":
        return plan

    inner = plan.inputs()[0]
    if type(inner.to_variant()).__name__ != "Projection":
        return plan

    grandchild = inner.inputs()[0]
    if type(grandchild.to_variant()).__name__ != "EmptyRelation":
        return plan

    table_name = variant.alias()

    # Extract schema by executing the projection (0 rows, correct types)
    df = ctx.create_dataframe_from_logical_plan(inner)
    schema = df.schema()

    # Register a real empty table so ctx.table() returns a proper TableScan
    empty_arrays = [pa.array([], type=schema.field(f).type) for f in schema.names]
    empty_batch = pa.record_batch(empty_arrays, schema=schema)
    ctx.register_record_batches(table_name, [[empty_batch]])

    return ctx.table(table_name).logical_plan()


def main():
    plan_bytes_path = Path(__file__).parent / "plan_bytes.bin"
    if not plan_bytes_path.exists():
        raise FileNotFoundError(
            f"{plan_bytes_path} not found. "
            "Run `cargo test -p vegafusion-runtime --test test_plan_serialization` first."
        )

    ctx = SessionContext()

    data = plan_bytes_path.read_bytes()
    plan = LogicalPlan.from_proto(ctx, data)

    print("Deserialized plan:")
    print(plan.display_indent())
    print()

    new_plan = rewrite_view_to_empty_table(ctx, plan)

    print("After rewrite:")
    print(new_plan.display_indent())
    print(f"Plan type: {type(new_plan.to_variant()).__name__}")


if __name__ == "__main__":
    main()
