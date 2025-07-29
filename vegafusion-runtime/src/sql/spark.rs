use datafusion::sql::unparser::dialect::CustomDialectBuilder;
use datafusion::sql::unparser::Unparser;
use datafusion_expr::LogicalPlan;
use sqlparser::ast::{visit_expressions_mut, Expr as AstExpr, Ident, OrderByExpr, OrderByOptions, Statement, WindowType};
use std::ops::ControlFlow;
use vegafusion_common::error::{Result, VegaFusionError};

/// This method converts a logical plan, which we get from DataFusion, into a SQL query
/// which is compatible with Spark. 
// The SQL generated from the DataFusion plan is not compatible with Spark by default.
// To make it work, we take the logical plan and apply changes to either the logical plan 
// itself or to the abstract syntax tree generated from this logical plan before converting 
// it into an SQL string. This allows us to rewrite parts of the plan or syntax tree to 
// be compatible with Spark.
pub fn logical_plan_to_spark_sql(plan: &LogicalPlan) -> Result<String> {
    let dialect = CustomDialectBuilder::new().build();
    let unparser = Unparser::new(&dialect).with_pretty(true);
    let mut ast = unparser.plan_to_sql(plan).map_err(|e| {
        VegaFusionError::unparser(format!("Failed to generate SQL AST from logical plan: {}", e))
    })?;

    println!("AST before processing");
    println!("{:#?}", ast);

    rewrite_row_number(&mut ast);

    println!("===============================");
    println!("AST after processing");
    println!("{:#?}", ast);

    let spark_sql = ast.to_string();

    Ok(spark_sql)
}

/// When adding row_number() DataFusion generates SQL like this:
/// `row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING``
/// Which is not compatible with Spark. For Spark we rewrite AST to be
/// `row_number() OVER (ORDER BY monotonically_increasing_id())``
fn rewrite_row_number(ast: &mut Statement) {
    visit_expressions_mut(ast, |expr: &mut AstExpr| {
        if let AstExpr::Function(func) = expr {
            if func.name.to_string().to_lowercase() == "row_number" {
                if let Some(WindowType::WindowSpec(ref mut window_spec)) = &mut func.over {
                    window_spec.window_frame = None;
                    window_spec.order_by = vec![OrderByExpr {
                        expr: AstExpr::Identifier(Ident::new("monotonically_increasing_id()")),
                        options: OrderByOptions {
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
