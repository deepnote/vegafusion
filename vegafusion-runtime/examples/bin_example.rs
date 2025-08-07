use std::{collections::HashMap, sync::Arc};
use vegafusion_core::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use vegafusion_core::proto::gen::pretransform::PreTransformLogicalPlanOpts;
use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_runtime::sql::logical_plan_to_spark_sql;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

/// This example demonstrates how to use the `pre_transform_spec` method to create a new
/// spec with supported transforms pre-evaluated.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spec = get_spec();

    let runtime = VegaFusionRuntime::new(None);
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_age", DataType::Int64, false),
        Field::new("customer_name", DataType::Utf8, true),
    ]));

    let mut datasets: HashMap<String, SchemaRef> = HashMap::new();
    datasets.insert("sales_data_1kk".to_string(), schema);

    let options = PreTransformLogicalPlanOpts {
        local_tz: "UTC".to_string(),
        default_input_tz: None,
        preserve_interactivity: false,
        keep_variables: vec![],
    };

    let (_transformed_spec, transformed_datasets, warnings) = runtime
        .pre_transform_logical_plan(&spec, datasets, &options)
        .await
        .unwrap();

    assert_eq!(warnings.len(), 0);

    for export_update in &transformed_datasets {
        // println!("Got export update {}", export_update.name);
        println!("Got export {:#?}", export_update);
    }

    let plan = match &transformed_datasets[1].value {
        TaskValue::Plan(logical_plan) => logical_plan.clone(),
        _ => panic!("Expected Plan variant in transformed_datasets[1].value"),
    };

    let sql = logical_plan_to_spark_sql(&plan)?;
    println!("{}", sql);

    Ok(())
}

fn get_spec() -> ChartSpec {
    let spec_str = r##"
    {
  "$schema": "https://vega.github.io/schema/vega/v6.json",
  "autosize": {"type": "fit", "contains": "padding"},
  "background": "white",
  "padding": 5,
  "style": "cell",
  "data": [
    {"name": "legend_size_0_store"},
    {"name": "legend_color_0_store"},
    {"name": "interval_selection_store"},
    {
      "name": "source_0",
      "url": "vegafusion+dataset://sales_data_1kk",
      "format": {"type": "json"},
      "transform": [
        {
          "type": "extent",
          "field": "customer_age",
          "signal": "layer_0_layer_0_layer_0_bin_step_10_customer_age_extent"
        },
        {
          "type": "bin",
          "field": "customer_age",
          "as": ["bin_step_10_customer_age", "bin_step_10_customer_age_end"],
          "signal": "layer_0_layer_0_layer_0_bin_step_10_customer_age_bins",
          "extent": {
            "signal": "layer_0_layer_0_layer_0_bin_step_10_customer_age_extent"
          },
          "step": 10
        },
        {
          "type": "aggregate",
          "groupby": [
            "bin_step_10_customer_age",
            "bin_step_10_customer_age_end"
          ],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_step_10_customer_age\"]) && isFinite(+datum[\"bin_step_10_customer_age\"])"
        }
      ]
    }
  ],
  "signals": [
    {
      "name": "width",
      "init": "isFinite(containerSize()[0]) ? containerSize()[0] : 300",
      "on": [
        {
          "update": "isFinite(containerSize()[0]) ? containerSize()[0] : 300",
          "events": "window:resize"
        }
      ]
    },
    {
      "name": "height",
      "init": "isFinite(containerSize()[1]) ? containerSize()[1] : 300",
      "on": [
        {
          "update": "isFinite(containerSize()[1]) ? containerSize()[1] : 300",
          "events": "window:resize"
        }
      ]
    },
    {
      "name": "unit",
      "value": {},
      "on": [
        {"events": "pointermove", "update": "isTuple(group()) ? group() : unit"}
      ]
    },
    {
      "name": "legend_size_0",
      "update": "vlSelectionResolve(\"legend_size_0_store\", \"union\", true, true)"
    },
    {
      "name": "legend_color_0",
      "update": "vlSelectionResolve(\"legend_color_0_store\", \"union\", true, true)"
    },
    {
      "name": "interval_selection",
      "update": "vlSelectionResolve(\"interval_selection_store\", \"union\")"
    },
    {
      "name": "legend_size_0_tuple",
      "on": [
        {
          "events": [{"source": "scope", "type": "click"}],
          "update": "datum && item().mark.marktype !== 'group' && indexof(item().mark.role, 'legend') < 0 && indexof(item().mark.name, 'interval_selection_brush') < 0 ? {unit: \"layer_0_layer_0_layer_0\", fields: legend_size_0_tuple_fields, values: []} : null",
          "force": true
        },
        {"events": [{"source": "view", "type": "dblclick"}], "update": "null"}
      ]
    },
    {"name": "legend_size_0_tuple_fields", "value": []},
    {
      "name": "legend_size_0_toggle",
      "value": false,
      "on": [
        {"events": [{"source": "scope", "type": "click"}], "update": "true"},
        {"events": [{"source": "view", "type": "dblclick"}], "update": "false"}
      ]
    },
    {
      "name": "legend_size_0_modify",
      "on": [
        {
          "events": {"signal": "legend_size_0_tuple"},
          "update": "modify(\"legend_size_0_store\", legend_size_0_toggle ? null : legend_size_0_tuple, legend_size_0_toggle ? null : true, legend_size_0_toggle ? legend_size_0_tuple : null)"
        }
      ]
    },
    {
      "name": "legend_color_0_tuple",
      "on": [
        {
          "events": [{"source": "scope", "type": "click"}],
          "update": "datum && item().mark.marktype !== 'group' && indexof(item().mark.role, 'legend') < 0 && indexof(item().mark.name, 'interval_selection_brush') < 0 ? {unit: \"layer_0_layer_0_layer_0\", fields: legend_color_0_tuple_fields, values: []} : null",
          "force": true
        },
        {"events": [{"source": "view", "type": "dblclick"}], "update": "null"}
      ]
    },
    {"name": "legend_color_0_tuple_fields", "value": []},
    {
      "name": "legend_color_0_toggle",
      "value": false,
      "on": [
        {"events": [{"source": "scope", "type": "click"}], "update": "true"},
        {"events": [{"source": "view", "type": "dblclick"}], "update": "false"}
      ]
    },
    {
      "name": "legend_color_0_modify",
      "on": [
        {
          "events": {"signal": "legend_color_0_tuple"},
          "update": "modify(\"legend_color_0_store\", legend_color_0_toggle ? null : legend_color_0_tuple, legend_color_0_toggle ? null : true, legend_color_0_toggle ? legend_color_0_tuple : null)"
        }
      ]
    },
    {
      "name": "interval_selection_x",
      "value": [],
      "on": [
        {
          "events": {
            "source": "scope",
            "type": "pointerdown",
            "filter": [
              "!event.item || event.item.mark.name !== \"interval_selection_brush\""
            ]
          },
          "update": "[x(unit), x(unit)]"
        },
        {
          "events": {
            "source": "window",
            "type": "pointermove",
            "consume": true,
            "between": [
              {
                "source": "scope",
                "type": "pointerdown",
                "filter": [
                  "!event.item || event.item.mark.name !== \"interval_selection_brush\""
                ]
              },
              {"source": "window", "type": "pointerup"}
            ]
          },
          "update": "[interval_selection_x[0], clamp(x(unit), 0, width)]"
        },
        {
          "events": {"signal": "interval_selection_scale_trigger"},
          "update": "[scale(\"x\", interval_selection_customer_age[0]), scale(\"x\", interval_selection_customer_age[1])]"
        },
        {
          "events": [{"source": "view", "type": "dblclick"}],
          "update": "[0, 0]"
        },
        {
          "events": {"signal": "interval_selection_translate_delta"},
          "update": "clampRange(panLinear(interval_selection_translate_anchor.extent_x, interval_selection_translate_delta.x / span(interval_selection_translate_anchor.extent_x)), 0, width)"
        },
        {
          "events": {"signal": "interval_selection_zoom_delta"},
          "update": "clampRange(zoomLinear(interval_selection_x, interval_selection_zoom_anchor.x, interval_selection_zoom_delta), 0, width)"
        }
      ]
    },
    {
      "name": "interval_selection_customer_age",
      "on": [
        {
          "events": {"signal": "interval_selection_x"},
          "update": "interval_selection_x[0] === interval_selection_x[1] ? null : invert(\"x\", interval_selection_x)"
        }
      ]
    },
    {
      "name": "interval_selection_scale_trigger",
      "value": {},
      "on": [
        {
          "events": [{"scale": "x"}],
          "update": "(!isArray(interval_selection_customer_age) || (+invert(\"x\", interval_selection_x)[0] === +interval_selection_customer_age[0] && +invert(\"x\", interval_selection_x)[1] === +interval_selection_customer_age[1])) ? interval_selection_scale_trigger : {}"
        }
      ]
    },
    {
      "name": "interval_selection_tuple",
      "on": [
        {
          "events": [{"signal": "interval_selection_customer_age"}],
          "update": "interval_selection_customer_age ? {unit: \"layer_0_layer_0_layer_0\", fields: interval_selection_tuple_fields, values: [interval_selection_customer_age]} : null"
        }
      ]
    },
    {
      "name": "interval_selection_tuple_fields",
      "value": [{"field": "customer_age", "channel": "x", "type": "R"}]
    },
    {
      "name": "interval_selection_translate_anchor",
      "value": {},
      "on": [
        {
          "events": [
            {
              "source": "scope",
              "type": "pointerdown",
              "markname": "interval_selection_brush"
            }
          ],
          "update": "{x: x(unit), y: y(unit), extent_x: slice(interval_selection_x)}"
        }
      ]
    },
    {
      "name": "interval_selection_translate_delta",
      "value": {},
      "on": [
        {
          "events": [
            {
              "source": "window",
              "type": "pointermove",
              "consume": true,
              "between": [
                {
                  "source": "scope",
                  "type": "pointerdown",
                  "markname": "interval_selection_brush"
                },
                {"source": "window", "type": "pointerup"}
              ]
            }
          ],
          "update": "{x: interval_selection_translate_anchor.x - x(unit), y: interval_selection_translate_anchor.y - y(unit)}"
        }
      ]
    },
    {
      "name": "interval_selection_zoom_anchor",
      "on": [
        {
          "events": [
            {
              "source": "scope",
              "type": "wheel",
              "consume": true,
              "markname": "interval_selection_brush"
            }
          ],
          "update": "{x: x(unit), y: y(unit)}"
        }
      ]
    },
    {
      "name": "interval_selection_zoom_delta",
      "on": [
        {
          "events": [
            {
              "source": "scope",
              "type": "wheel",
              "consume": true,
              "markname": "interval_selection_brush"
            }
          ],
          "force": true,
          "update": "pow(1.001, event.deltaY * pow(16, event.deltaMode))"
        }
      ]
    },
    {
      "name": "interval_selection_modify",
      "on": [
        {
          "events": {"signal": "interval_selection_tuple"},
          "update": "modify(\"interval_selection_store\", interval_selection_tuple, true)"
        }
      ]
    }
  ],
  "marks": [
    {
      "name": "interval_selection_brush_bg",
      "type": "rect",
      "clip": true,
      "encode": {
        "enter": {"fill": {"value": "#333"}, "fillOpacity": {"value": 0.125}},
        "update": {
          "x": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "signal": "interval_selection_x[0]"
            },
            {"value": 0}
          ],
          "y": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "value": 0
            },
            {"value": 0}
          ],
          "x2": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "signal": "interval_selection_x[1]"
            },
            {"value": 0}
          ],
          "y2": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "field": {"group": "height"}
            },
            {"value": 0}
          ]
        }
      }
    },
    {
      "name": "layer_0_layer_0_layer_0_marks",
      "type": "rect",
      "clip": true,
      "style": ["bar"],
      "interactive": true,
      "from": {"data": "source_0"},
      "encode": {
        "update": {
          "tooltip": {
            "signal": "{\"customer_age (binned)\": !isValid(datum[\"bin_step_10_customer_age\"]) || !isFinite(+datum[\"bin_step_10_customer_age\"]) ? \"null\" : format(datum[\"bin_step_10_customer_age\"], \"\") + \" – \" + format(datum[\"bin_step_10_customer_age_end\"], \"\"), \"Count of Records\": numberFormatFromNumberType(datum[\"__count\"], {\"decimals\":null,\"type\":\"default\"})}"
          },
          "fill": {"scale": "layer_0_layer_0_color", "value": "Orders"},
          "opacity": [
            {
              "test": "(!length(data(\"legend_size_0_store\")) || vlSelectionTest(\"legend_size_0_store\", datum)) && (!length(data(\"legend_color_0_store\")) || vlSelectionTest(\"legend_color_0_store\", datum)) && (!length(data(\"interval_selection_store\")) || vlSelectionTest(\"interval_selection_store\", datum))",
              "value": 1
            },
            {"value": 0.2}
          ],
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"customer_age (binned): \" + (!isValid(datum[\"bin_step_10_customer_age\"]) || !isFinite(+datum[\"bin_step_10_customer_age\"]) ? \"null\" : format(datum[\"bin_step_10_customer_age\"], \"\") + \" – \" + format(datum[\"bin_step_10_customer_age_end\"], \"\")) + \"; Count of Records: \" + (numberFormatFromNumberType(datum[\"__count\"], {\"decimals\":null,\"type\":\"default\"}))"
          },
          "x2": {
            "scale": "x",
            "field": "bin_step_10_customer_age",
            "offset": {
              "signal": "0.5 + (true ? -1 : 1) * (abs(scale(\"x\", datum[\"bin_step_10_customer_age_end\"]) - scale(\"x\", datum[\"bin_step_10_customer_age\"])) < 0.25 ? -0.5 * (0.25 - (abs(scale(\"x\", datum[\"bin_step_10_customer_age_end\"]) - scale(\"x\", datum[\"bin_step_10_customer_age\"])))) : 0.5)"
            }
          },
          "x": {
            "scale": "x",
            "field": "bin_step_10_customer_age_end",
            "offset": {
              "signal": "0.5 + (true ? -1 : 1) * (abs(scale(\"x\", datum[\"bin_step_10_customer_age_end\"]) - scale(\"x\", datum[\"bin_step_10_customer_age\"])) < 0.25 ? 0.5 * (0.25 - (abs(scale(\"x\", datum[\"bin_step_10_customer_age_end\"]) - scale(\"x\", datum[\"bin_step_10_customer_age\"])))) : -0.5)"
            }
          },
          "y": {"scale": "y", "field": "__count"},
          "y2": {"scale": "y", "value": 0}
        }
      }
    },
    {
      "name": "interval_selection_brush",
      "type": "rect",
      "clip": true,
      "encode": {
        "enter": {
          "cursor": {"value": "move"},
          "fill": {"value": "transparent"}
        },
        "update": {
          "x": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "signal": "interval_selection_x[0]"
            },
            {"value": 0}
          ],
          "y": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "value": 0
            },
            {"value": 0}
          ],
          "x2": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "signal": "interval_selection_x[1]"
            },
            {"value": 0}
          ],
          "y2": [
            {
              "test": "data(\"interval_selection_store\").length && data(\"interval_selection_store\")[0].unit === \"layer_0_layer_0_layer_0\"",
              "field": {"group": "height"}
            },
            {"value": 0}
          ],
          "stroke": [
            {
              "test": "interval_selection_x[0] !== interval_selection_x[1]",
              "value": "white"
            },
            {"value": null}
          ]
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "linear",
      "domain": {
        "signal": "[layer_0_layer_0_layer_0_bin_step_10_customer_age_bins.start, layer_0_layer_0_layer_0_bin_step_10_customer_age_bins.stop]"
      },
      "range": [0, {"signal": "width"}],
      "reverse": true,
      "bins": {
        "signal": "layer_0_layer_0_layer_0_bin_step_10_customer_age_bins"
      },
      "zero": false
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {"data": "source_0", "field": "__count"},
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    },
    {
      "name": "layer_0_layer_0_color",
      "type": "ordinal",
      "domain": ["Orders"],
      "range": ["#2266D3"]
    }
  ],
  "axes": [
    {
      "scale": "y",
      "orient": "left",
      "gridScale": "x",
      "grid": true,
      "tickCount": 5,
      "domain": false,
      "labels": false,
      "aria": false,
      "maxExtent": 0,
      "minExtent": 0,
      "ticks": false,
      "zindex": 0
    },
    {
      "scale": "x",
      "orient": "bottom",
      "grid": false,
      "title": "customer_age (binned)",
      "labelFlush": true,
      "labelOverlap": true,
      "tickCount": 5,
      "zindex": 0
    },
    {
      "scale": "y",
      "orient": "left",
      "grid": false,
      "title": "Count of Records",
      "labelOverlap": true,
      "tickCount": 5,
      "encode": {
        "labels": {
          "update": {
            "text": {
              "signal": "numberFormatFromNumberType(datum.value, {\"decimals\":null,\"type\":\"default\"})"
            }
          }
        }
      },
      "zindex": 0
    }
  ],
  "legends": [
    {
      "fill": "layer_0_layer_0_color",
      "symbolType": "square",
      "encode": {"symbols": {"update": {"opacity": {"value": 1}}}}
    }
  ],
  "config": {"customFormatTypes": true, "legend": {"disable": false}},
  "usermeta": {
    "seriesNames": ["Orders"],
    "seriesOrder": [0],
    "aditionalTypeInfo": {"histogramLayerIndexes": [0]},
    "specSchemaVersion": 2,
    "tooltipDefaultMode": true
  }
}
    "##;
    serde_json::from_str(spec_str).unwrap()
}
