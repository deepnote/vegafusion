"""Python counterpart to test_plan_serialization.rs.

Deserializes proto bytes produced by the Rust test and rewrites the
ListingTable placeholder back into a real registered table. This verifies
that plans serialized in Rust can be consumed in Python using only the
default datafusion-proto codec (no custom LogicalExtensionCodec).

Run the Rust test first to generate plan_bytes.bin:
    cargo test -p vegafusion-runtime --test test_plan_serialization

The Rust side rewrites EmptyTable scans into ListingTable scans pointing at
a dummy path before serialization. After deserialization the plan is a normal
TableScan, but its underlying ListingTable would fail if executed (no real file).
"""

from pathlib import Path

import pyarrow as pa
from datafusion import LogicalPlan, SessionContext
from datafusion import unparser


def register_placeholder_tables(
    ctx: SessionContext,
    plan: LogicalPlan,
    placeholder_names: set[str],
) -> None:
    """Walk the plan tree and register real empty tables for placeholder scans.

    The Python datafusion bindings don't expose the TableScan's underlying
    provider, so we can't detect the placeholder URL directly. Instead the
    caller provides the set of table names that are known placeholders.

    We walk the tree, find leaf TableScans whose name is in placeholder_names,
    infer the schema, and register a real empty table under that name.
    """
    variant = plan.to_variant()
    name = type(variant).__name__

    if name == "TableScan":
        table_name = variant.table_name()
        if table_name in placeholder_names:
            # Get schema from the plan node
            df = ctx.create_dataframe_from_logical_plan(plan)
            arrow_schema = df.schema()

            empty_arrays = [
                pa.array([], type=arrow_schema.field(f).type)
                for f in arrow_schema.names
            ]
            empty_batch = pa.record_batch(empty_arrays, schema=arrow_schema)
            ctx.register_record_batches(table_name, [[empty_batch]])
        return

    # Recurse into children
    for child in plan.inputs():
        register_placeholder_tables(ctx, child, placeholder_names)


def rewrite_plan_via_sql(
    ctx: SessionContext,
    plan: LogicalPlan,
) -> LogicalPlan:
    """Convert plan to SQL and re-parse it against ctx's registered tables.

    After registering real empty tables for all placeholders, we unparse
    the plan to SQL and re-parse it. This gives us a clean plan that
    references the registered tables instead of the ListingTable placeholders.

    We use this SQL roundtrip because datafusion-python's LogicalPlan API is
    read-only — there's no TreeNodeRewriter, with_new_children(), or any other
    way to replace nodes in the plan tree directly.
    """
    u = unparser.Unparser(unparser.Dialect.default())
    sql = u.plan_to_sql(plan)
    print(f"Unparsed SQL: {sql}")
    return ctx.sql(sql).logical_plan()


def test_simple_plan():
    """Deserialize and rewrite the simple TableScan plan."""
    plan_bytes_path = Path(__file__).parent / "plan_bytes.bin"
    if not plan_bytes_path.exists():
        raise FileNotFoundError(
            f"{plan_bytes_path} not found. "
            "Run `cargo test -p vegafusion-runtime --test test_plan_serialization` first."
        )

    ctx = SessionContext()
    data = plan_bytes_path.read_bytes()
    plan = LogicalPlan.from_proto(ctx, data)

    print("=== Simple plan ===")
    print("Deserialized plan:")
    print(plan.display_indent())
    print()

    register_placeholder_tables(ctx, plan, {"movies"})
    new_plan = rewrite_plan_via_sql(ctx, plan)

    print("After rewrite:")
    print(new_plan.display_indent())
    print(f"Plan type: {type(new_plan.to_variant()).__name__}")
    print()


def test_complex_plan():
    """Deserialize and rewrite the complex Sort->Filter->TableScan plan."""
    plan_bytes_path = Path(__file__).parent / "plan_bytes_complex.bin"
    if not plan_bytes_path.exists():
        raise FileNotFoundError(
            f"{plan_bytes_path} not found. "
            "Run `cargo test -p vegafusion-runtime --test test_plan_serialization` first."
        )

    ctx = SessionContext()
    data = plan_bytes_path.read_bytes()
    plan = LogicalPlan.from_proto(ctx, data)

    print("=== Complex plan ===")
    print("Deserialized plan:")
    print(plan.display_indent())
    print()

    register_placeholder_tables(ctx, plan, {"movies"})
    new_plan = rewrite_plan_via_sql(ctx, plan)

    print("After rewrite:")
    print(new_plan.display_indent())
    print()


if __name__ == "__main__":
    test_simple_plan()
    test_complex_plan()
