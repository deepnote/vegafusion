import json
from typing import Any

from pprint import pprint
import pandas as pd
import vegafusion as vf


def main():
    spec = get_spec()
    print('Helloooooo')
    
    # Create orders DataFrame with correct schema but zero rows
    orders_df = pd.DataFrame({
        'datetime': pd.Series([], dtype='datetime64[ns]'),
        'product_category': pd.Series([], dtype='object'),
        'product_name': pd.Series([], dtype='object'),
        'price': pd.Series([], dtype='float64'),
        'quantity': pd.Series([], dtype='int64'),
        'discount': pd.Series([], dtype='float64'),
        'discount_type': pd.Series([], dtype='object'),
        'order_total': pd.Series([], dtype='float64'),
        'customer_name': pd.Series([], dtype='object'),
        'customer_email': pd.Series([], dtype='object'),
        'customer_age': pd.Series([], dtype='int64'),
        'customer_country': pd.Series([], dtype='object'),
        'customer_segment': pd.Series([], dtype='object'),
        'satisfaction_score': pd.Series([], dtype='int64')
    })
    
    transformed_spec, datasets, warnings = vf.runtime.pre_transform_logical_plan(
        spec, inline_datasets={"orders": orders_df}
    )
    assert warnings == []
    # for ds in datasets:
    ds = datasets[0]
    print('Dataset', ds['name'])
    print(ds['sql'])
    print('---------------------')
    print(ds['logical_plan'])


def get_spec() -> dict[str, Any]:
    # "url": "vegafusion+dataset://orders",
    spec_str = r"""
{
  "$schema": "https://vega.github.io/schema/vega/v6.json",
  "background": "white",
  "padding": 5,
  "width": 300,
  "height": 300,
  "style": "cell",
  "data": [
    {
      "name": "data_0",
      "url": "vegafusion+dataset://orders",
      "transform": [
        {
          "type": "formula",
          "expr": "toDate(datum[\"datetime\"])",
          "as": "datetime"
        },
        {
          "field": "datetime",
          "type": "timeunit",
          "units": ["year", "month"],
          "as": ["yearmonth_datetime", "yearmonth_datetime_end"]
        },
        {
          "type": "formula",
          "expr": "0.5 * timeOffset('month', datum['yearmonth_datetime'], -1) + 0.5 * datum['yearmonth_datetime']",
          "as": "yearmonth_datetime_offsetted_rect_start"
        },
        {
          "type": "formula",
          "expr": "0.5 * datum['yearmonth_datetime'] + 0.5 * datum['yearmonth_datetime_end']",
          "as": "yearmonth_datetime_offsetted_rect_end"
        },
        {
          "type": "aggregate",
          "groupby": [
            "yearmonth_datetime",
            "yearmonth_datetime_end",
            "yearmonth_datetime_offsetted_rect_start",
            "yearmonth_datetime_offsetted_rect_end"
          ],
          "ops": ["sum"],
          "fields": ["order_total"],
          "as": ["sum_order_total"]
        },
        {
          "type": "filter",
          "expr": "(isDate(datum[\"yearmonth_datetime\"]) || (isValid(datum[\"yearmonth_datetime\"]) && isFinite(+datum[\"yearmonth_datetime\"]))) && isValid(datum[\"sum_order_total\"]) && isFinite(+datum[\"sum_order_total\"])"
        }
      ]
    }
  ],
  "marks": [
    {
      "name": "layer_0_layer_0_layer_0_marks",
      "type": "rect",
      "clip": true,
      "style": ["bar"],
      "from": {"data": "data_0"},
      "encode": {
        "update": {
          "tooltip": {
            "signal": "{\"datetime (year-month)\": timeFormat(datum[\"yearmonth_datetime\"], timeUnitSpecifier([\"year\",\"month\"], {\"year-month\":\"%b %Y \",\"year-month-date\":\"%b %d, %Y \"})), \"Sum of order_total\": format(datum[\"sum_order_total\"], \"\")}"
          },
          "fill": {"scale": "layer_0_layer_0_color", "value": "order_total"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"datetime (year-month): \" + (timeFormat(datum[\"yearmonth_datetime\"], timeUnitSpecifier([\"year\",\"month\"], {\"year-month\":\"%b %Y \",\"year-month-date\":\"%b %d, %Y \"}))) + \"; Sum of order_total: \" + (format(datum[\"sum_order_total\"], \"\"))"
          },
          "xc": {
            "scale": "x",
            "field": "yearmonth_datetime",
            "offset": {"scale": "xOffset", "value": "series_0", "band": 0.5}
          },
          "width": {"signal": "max(0.25, bandwidth('xOffset'))"},
          "y": {"scale": "y", "field": "sum_order_total"},
          "y2": {"scale": "y", "value": 0}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "time",
      "domain": {
        "data": "data_0",
        "fields": [
          "yearmonth_datetime_offsetted_rect_start",
          "yearmonth_datetime_offsetted_rect_end"
        ]
      },
      "range": [0, {"signal": "width"}]
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {"data": "data_0", "field": "sum_order_total"},
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    },
    {
      "name": "xOffset",
      "type": "band",
      "domain": ["series_0"],
      "range": [
        {
          "signal": "-0.4 * (scale('x', datetime(2001, 1, 1, 0, 0, 0, 0)) - scale('x', datetime(2001, 0, 1, 0, 0, 0, 0)))"
        },
        {
          "signal": "0.4 * (scale('x', datetime(2001, 1, 1, 0, 0, 0, 0)) - scale('x', datetime(2001, 0, 1, 0, 0, 0, 0)))"
        }
      ]
    },
    {
      "name": "layer_0_layer_0_color",
      "type": "ordinal",
      "domain": ["order_total"],
      "range": ["#2266D3"]
    }
  ],
  "axes": [
    {
      "scale": "y",
      "orient": "left",
      "gridScale": "x",
      "grid": true,
      "tickCount": {"signal": "ceil(height/40)"},
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
      "title": "datetime (year-month)",
      "format": {
        "signal": "timeUnitSpecifier([\"year\",\"month\"], {\"year-month\":\"%b %Y \",\"year-month-date\":\"%b %d, %Y \"})"
      },
      "labelFlush": true,
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(width/40)"},
      "tickMinStep": {
        "signal": "datetime(2001, 1, 1, 0, 0, 0, 0) - datetime(2001, 0, 1, 0, 0, 0, 0)"
      },
      "zindex": 0
    },
    {
      "scale": "y",
      "orient": "left",
      "grid": false,
      "title": "Sum of order_total",
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(height/40)"},
      "zindex": 0
    }
  ],
  "legends": [{"fill": "layer_0_layer_0_color", "symbolType": "square"}],
  "config": {"legend": {"disable": false}},
  "usermeta": {
    "seriesNames": ["order_total"],
    "seriesOrder": [0],
    "specSchemaVersion": 2,
    "tooltipDefaultMode": true
  }
}
"""
    return json.loads(spec_str)


if __name__ == "__main__":
    main()
