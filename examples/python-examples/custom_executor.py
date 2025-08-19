import vegafusion as vf
import pyarrow as pa


def simple_logging_executor(logical_plan_json: str) -> pa.Table:
    """A minimal executor that logs and throws unimplemented error."""
    # TODO: Update signature once we pass logical plan as proper structure instead of string
    print("📋 Custom executor received logical plan:")
    print(f"   Plan length: {len(logical_plan_json)} characters")
    print(f"   Plan preview: {logical_plan_json[:100]}...")

    raise NotImplementedError("Custom executor is not implemented yet!")


# This example shows the simplest possible custom executor that logs
# received plans and throws an unimplemented error.
def main():
    """Demonstrate the minimal custom executor."""

    spec = get_spec()

    schema = pa.schema(
        [
            pa.field("Title", pa.string()),
            pa.field("US Gross", pa.int64()),
            pa.field("Worldwide Gross", pa.int64()),
            pa.field("US DVD Sales", pa.int64()),
            pa.field("Production Budget", pa.int64()),
            pa.field("Release Date", pa.date32()),
            pa.field("MPAA Rating", pa.string()),
            pa.field("Running Time min", pa.int32()),
            pa.field("Distributor", pa.string()),
            pa.field("Source", pa.string()),
            pa.field("Major Genre", pa.string()),
            pa.field("Creative Type", pa.string()),
            pa.field("Director", pa.string()),
            pa.field("Rotten Tomatoes Rating", pa.int8()),
            pa.field("IMDB Rating", pa.float32()),
            pa.field("IMDB Votes", pa.int64()),
        ]
    )

    print("Testing custom executor with VegaFusion...")

    # Create a DataFusion runtime that uses a custom Python executor
    runtime = vf.VegaFusionRuntime(
        executor=simple_logging_executor,
    )

    try:
        runtime.pre_transform_spec(
            spec=spec,
            local_tz="UTC",
            inline_datasets={"movies": schema},
        )
    except ValueError as e:
        print(f"✅ Expected error caught: {e}")

    print("Done!")


def get_spec():
    """
    Based on https://vega.github.io/editor/#/examples/vega/histogram-null-values
    """
    return {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "description": "A histogram of film ratings, modified to include null values.",
        "width": 400,
        "height": 200,
        "padding": 5,
        "autosize": {"type": "fit", "resize": True},
        "signals": [
            {
                "name": "maxbins",
                "value": 10,
                "bind": {"input": "select", "options": [5, 10, 20]},
            },
            {"name": "binCount", "update": "(bins.stop - bins.start) / bins.step"},
            {"name": "nullGap", "value": 10},
            {"name": "barStep", "update": "(width - nullGap) / (1 + binCount)"},
        ],
        "data": [
            {
                "name": "table",
                "url": "vegafusion+dataset://movies",
                "transform": [
                    {"type": "extent", "field": "IMDB Rating", "signal": "extent"},
                    {
                        "type": "bin",
                        "signal": "bins",
                        "field": "IMDB Rating",
                        "extent": {"signal": "extent"},
                        "maxbins": {"signal": "maxbins"},
                    },
                ],
            },
            {
                "name": "counts",
                "source": "table",
                "transform": [
                    {"type": "filter", "expr": "datum['IMDB Rating'] != null"},
                    {"type": "aggregate", "groupby": ["bin0", "bin1"]},
                ],
            },
            {
                "name": "nulls",
                "source": "table",
                "transform": [
                    {"type": "filter", "expr": "datum['IMDB Rating'] == null"},
                    {"type": "aggregate", "groupby": []},
                ],
            },
        ],
        "scales": [
            {
                "name": "yscale",
                "type": "linear",
                "range": "height",
                "round": True,
                "nice": True,
                "domain": {
                    "fields": [
                        {"data": "counts", "field": "count"},
                        {"data": "nulls", "field": "count"},
                    ]
                },
            },
            {
                "name": "xscale",
                "type": "linear",
                "range": [{"signal": "barStep + nullGap"}, {"signal": "width"}],
                "round": True,
                "domain": {"signal": "[bins.start, bins.stop]"},
                "bins": {"signal": "bins"},
            },
            {
                "name": "xscale-null",
                "type": "band",
                "range": [0, {"signal": "barStep"}],
                "round": True,
                "domain": [None],
            },
        ],
        "axes": [
            {"orient": "bottom", "scale": "xscale", "tickMinStep": 0.5},
            {"orient": "bottom", "scale": "xscale-null"},
            {"orient": "left", "scale": "yscale", "tickCount": 5, "offset": 5},
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "counts"},
                "encode": {
                    "update": {
                        "x": {"scale": "xscale", "field": "bin0", "offset": 1},
                        "x2": {"scale": "xscale", "field": "bin1"},
                        "y": {"scale": "yscale", "field": "count"},
                        "y2": {"scale": "yscale", "value": 0},
                        "fill": {"value": "steelblue"},
                    },
                    "hover": {"fill": {"value": "firebrick"}},
                },
            },
            {
                "type": "rect",
                "from": {"data": "nulls"},
                "encode": {
                    "update": {
                        "x": {"scale": "xscale-null", "value": None, "offset": 1},
                        "x2": {"scale": "xscale-null", "band": 1},
                        "y": {"scale": "yscale", "field": "count"},
                        "y2": {"scale": "yscale", "value": 0},
                        "fill": {"value": "#aaa"},
                    },
                    "hover": {"fill": {"value": "firebrick"}},
                },
            },
        ],
    }


if __name__ == "__main__":
    main()
