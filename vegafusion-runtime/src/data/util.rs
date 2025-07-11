use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::{col, Expr};

pub struct PureAggRewriter {
    pub pure_aggs: Vec<Expr>,
    pub next_id: usize,
}

impl Default for PureAggRewriter {
    fn default() -> Self {
        Self::new()
    }
}

impl PureAggRewriter {
    pub fn new() -> Self {
        Self {
            pure_aggs: vec![],
            next_id: 0,
        }
    }

    fn new_agg_name(&mut self) -> String {
        let name = format!("_agg_{}", self.next_id);
        self.next_id += 1;
        name
    }
}

impl TreeNodeRewriter for PureAggRewriter {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let Expr::AggregateFunction(agg) = node {
            // extract agg and replace with column
            let name = self.new_agg_name();
            self.pure_aggs
                .push(Expr::AggregateFunction(agg).alias(&name));
            Ok(Transformed::new_transformed(col(name), true))
        } else {
            // Return expr node unchanged
            Ok(Transformed::no(node))
        }
    }
}
