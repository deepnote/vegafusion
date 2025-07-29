use datafusion::sql::unparser::dialect::CustomDialectBuilder;
use datafusion::sql::unparser::Unparser;
use datafusion_expr::{LogicalPlan, Expr};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column};
use sqlparser::ast::{self, visit_expressions_mut};
use std::ops::ControlFlow;
use vegafusion_common::error::{Result, VegaFusionError};

/// This method converts a logical plan, which we get from DataFusion, into a SQL query
/// which is compatible with Spark. 
// The SQL generated from the DataFusion plan is not compatible with Spark by default.
// To make it work, we apply changes to both the logical plan itself and to the 
// abstract syntax tree generated from this logical plan before converting 
// it into an SQL string. This allows us to rewrite parts of the plan or syntax tree to 
// be compatible with Spark.
pub fn logical_plan_to_spark_sql(plan: &LogicalPlan) -> Result<String> {
    println!("Plan before processing");
    println!("{:#?}", plan);

    let plan = plan.clone();
    let processed_plan = rewrite_subquery_column_identifiers(plan)?;

    println!("===============================");
    println!("Plan after processing");
    println!("{:#?}", processed_plan);

    let dialect = CustomDialectBuilder::new().build();
    let unparser = Unparser::new(&dialect).with_pretty(true);
    let mut statement = unparser.plan_to_sql(&processed_plan).map_err(|e| {
        VegaFusionError::unparser(format!("Failed to generate SQL AST from logical plan: {}", e))
    })?;

    println!("===============================");
    println!("AST before processing");
    println!("{:#?}", statement);

    rewrite_row_number(&mut statement);
    rewrite_inf_and_nan(&mut statement);

    println!("===============================");
    println!("AST after processing");
    println!("{:#?}", statement);

    let spark_sql = statement.to_string();

    Ok(spark_sql)
}

/// When adding row_number() DataFusion generates SQL like this:
/// ```
/// row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
/// ```
/// Which is not compatible with Spark. For Spark we rewrite AST to be
/// ```
/// row_number() OVER (ORDER BY monotonically_increasing_id())
/// ```
fn rewrite_row_number(statement: &mut ast::Statement) {
    visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Function(func) = expr {
            if func.name.to_string().to_lowercase() == "row_number" {
                if let Some(ast::WindowType::WindowSpec(ref mut window_spec)) = &mut func.over {
                    window_spec.window_frame = None;
                    window_spec.order_by = vec![ast::OrderByExpr {
                        expr: ast::Expr::Identifier(ast::Ident::new("monotonically_increasing_id()")),
                        options: ast::OrderByOptions {
                            asc: None,
                            nulls_first: None,
                        },  
                        with_fill: None 
                    }];
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// When DataFusion generates SQL, NaN and infinity values are presented as
/// literals, while Spark requires them to be `float('NaN')`, `float('inf')`, etc.
fn rewrite_inf_and_nan(statement: &mut ast::Statement) {
    const SPECIAL_VALUES: &[&str] = &["nan", "inf", "infinity", "+inf", "+infinity", "-inf", "-infinity"];
    
    visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Value(value) = expr {
            if let ast::Value::Number(num_str, _) = &value.value {
                if SPECIAL_VALUES.contains(&num_str.to_lowercase().as_str()) {
                    *expr = ast::Expr::Function(ast::Function {
                        name: ast::ObjectName::from(vec![ast::Ident::new("float")]),
                        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                ast::Expr::Value(ast::ValueWithSpan {
                                    value: ast::Value::SingleQuotedString(num_str.clone()),
                                    span: value.span.clone(),
                                })
                            ))],
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: vec![],
                        uses_odbc_syntax: false,
                        parameters: ast::FunctionArguments::None,
                    });
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// DataFusion logical plan which uses compound names when selecting from subquery:
/// ```
/// SELECT orders.customer_name, orders.customer_age FROM (SELECT orders.customer_name, orders.customer_age FROM orders)
/// ```
/// This is not a valid SQL, as `orders` isn't available once we get out of first query.
/// So we rewrite logical plan to replace compound names with just the column names in projections
/// that select data from another projection
fn rewrite_subquery_column_identifiers(plan: LogicalPlan) -> Result<LogicalPlan> {
    let processed_plan = plan.transform_up_with_subqueries(|p| {
        if let LogicalPlan::Projection(projection) = &p {
            // only touch projections that read from another projection
            if matches!(*projection.input, LogicalPlan::Projection { .. }) {
                let rewritten_exprs = projection.expr
                    .iter()
                    .map(|e| {
                        e.clone().transform_up(|mut ex| {
                            if let Expr::Column(c) = &mut ex {
                                *c = Column::from_name(c.name.clone());
                                Ok(Transformed::yes(ex))
                            } else {
                                Ok(Transformed::no(ex))
                            }
                        }).map(|t| t.data)
                    })
                    .collect::<std::result::Result<_, _>>()?;
                let new_plan_node = p.with_new_exprs(rewritten_exprs, vec![(*projection.input).clone()])?;
                return Ok(Transformed::yes(new_plan_node))
            }
        }

        Ok(Transformed::no(p))
    }).map_err(|e| {
        VegaFusionError::unparser(format!("Failed to rewrite subquery column identifiers: {}", e))
    })?.data;
    
    Ok(processed_plan)
}
