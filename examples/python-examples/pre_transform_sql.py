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
        'country': pd.Series([], dtype='object'),
        'customer_segment': pd.Series([], dtype='object'),
        'satisfaction_score': pd.Series([], dtype='int64')
    })
    
    spec, datasets, warnings = vf.runtime.pre_transform_logical_plan(
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
    """
    Based on https://vega.github.io/editor/#/examples/vega/histogram-null-values
    """
    spec_str = r"""
{
  "$schema": "https://vega.github.io/schema/vega/v6.json",
  "background": "white",
  "padding": 5,
  "height": 300,
  "style": "cell",
  "data": [
    {
      "name": "data_0",
      "url": "vegafusion+dataset://orders",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["product_category", "country"],
          "ops": ["sum"],
          "fields": ["order_total"],
          "as": ["sum_order_total"]
        },
        {
          "type": "stack",
          "groupby": ["product_category"],
          "field": "sum_order_total",
          "sort": {"field": ["country"], "order": ["descending"]},
          "as": ["sum_order_total_start", "sum_order_total_end"],
          "offset": "zero"
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"sum_order_total\"]) && isFinite(+datum[\"sum_order_total\"])"
        }
      ]
    }
  ],
  "signals": [
    {"name": "x_step", "value": 20},
    {
      "name": "width",
      "update": "bandspace(domain('x').length, 0.1, 0.05) * x_step"
    }
  ],
  "marks": [
    {
      "name": "marks",
      "type": "rect",
      "style": ["bar"],
      "from": {"data": "data_0"},
      "encode": {
        "update": {
          "fill": {"scale": "color", "field": "country"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"product_category: \" + (isValid(datum[\"product_category\"]) ? datum[\"product_category\"] : \"\"+datum[\"product_category\"]) + \"; Sum of order_total: \" + (format(datum[\"sum_order_total\"], \"\")) + \"; country: \" + (isValid(datum[\"country\"]) ? datum[\"country\"] : \"\"+datum[\"country\"])"
          },
          "x": {"scale": "x", "field": "product_category"},
          "width": {"signal": "max(0.25, bandwidth('x'))"},
          "y": {"scale": "y", "field": "sum_order_total_end"},
          "y2": {"scale": "y", "field": "sum_order_total_start"}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "band",
      "domain": {"data": "data_0", "field": "product_category", "sort": true},
      "range": {"step": {"signal": "x_step"}},
      "paddingInner": 0.1,
      "paddingOuter": 0.05
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {
        "data": "data_0",
        "fields": ["sum_order_total_start", "sum_order_total_end"]
      },
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    },
    {
      "name": "color",
      "type": "ordinal",
      "domain": {"data": "data_0", "field": "country", "sort": true},
      "range": "category"
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
      "title": "product_category",
      "labelAlign": "right",
      "labelAngle": 270,
      "labelBaseline": "middle",
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
  "legends": [{"fill": "color", "symbolType": "square", "title": "country"}]
}
"""
    return json.loads(spec_str)


if __name__ == "__main__":
    main()
