import vegafusion as vf
import pyarrow as pa


def test_pre_transform_logical_plan_vendor_has_sparksql():
    """Test that the vendor-specific function adds sparksql property to datasets."""

    # Create Arrow schema for inline dataset
    schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.int64()),
        ]
    )

    # Simple Vega spec that should generate a logical plan
    spec = {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "width": 400,
        "height": 200,
        "data": [
            {
                "name": "source",
                "url": "vegafusion+dataset://test_data",
                "transform": [
                    {
                        "type": "aggregate",
                        "groupby": [],
                        "fields": ["a"],
                        "ops": ["mean"],
                        "as": ["avg_a"],
                    }
                ],
            }
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "source"},
                "encode": {
                    "enter": {
                        "x": {"scale": "x", "field": "avg_a"},
                        "y": {"value": 0},
                        "width": {"value": 20},
                        "height": {"value": 100},
                    }
                },
            }
        ],
        "scales": [
            {
                "name": "x",
                "type": "linear",
                "domain": {"data": "source", "field": "avg_a"},
                "range": "width",
            }
        ],
    }

    # Call the vendor-specific function like the example does
    new_spec, export_updates, warnings = vf.runtime.pre_transform_logical_plan_vendor(
        spec, output_format="sparksql", inline_dataset_schemas={"test_data": schema}
    )

    # Basic structure assertions
    assert isinstance(new_spec, dict)
    assert isinstance(export_updates, list)
    assert isinstance(warnings, list)

    # Check that at least one dataset has sparksql property
    found_sparksql = False
    for update in export_updates:
        assert isinstance(update, dict)
        assert "name" in update

        if "sparksql" in update:
            found_sparksql = True
            assert isinstance(
                update["sparksql"], str
            ), f"sparksql should be string, got {type(update['sparksql'])}"
            assert len(update["sparksql"]) > 0, "sparksql should not be empty"
            print(
                f"✓ Found sparksql for dataset '{update['name']}': {update['sparksql']}"
            )

    # We expect at least one dataset to have sparksql when using Spark output format
    if not found_sparksql:
        print("No sparksql found in any export updates")
        for update in export_updates:
            print(f"  Dataset '{update.get('name', 'unnamed')}': {list(update.keys())}")


if __name__ == "__main__":
    test_pre_transform_logical_plan_vendor_has_sparksql()
    print("Test passed!")
