use datafusion::sql::unparser::dialect::CustomDialectBuilder;
use datafusion::sql::unparser::Unparser;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{expr::ScalarFunction, Expr, LogicalPlan};
use lazy_static::lazy_static;
use regex;
use sqlparser::ast::{self, visit_expressions_mut};
use std::collections::HashMap;
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
    let plan = plan.clone();
    let processed_plan = rewrite_subquery_column_identifiers(plan)?;
    let processed_plan = rewrite_datetime_formatting(processed_plan)?;

    let dialect = CustomDialectBuilder::new().build();
    let unparser = Unparser::new(&dialect).with_pretty(true);
    let mut statement = unparser.plan_to_sql(&processed_plan).map_err(|e| {
        VegaFusionError::vendor(format!(
            "Failed to generate SQL AST from logical plan: {}",
            e
        ))
    })?;

    rewrite_row_number(&mut statement);
    rewrite_inf_and_nan(&mut statement);
    rewrite_date_format(&mut statement);
    rewrite_timestamps(&mut statement);
    rewrite_intervals(&mut statement);
    rewrite_nested_is_null(&mut statement);
    rewrite_column_identifiers(&mut statement);

    let spark_sql = statement.to_string();

    Ok(spark_sql)
}

/// When adding row_number() DataFusion generates SQL like this:
/// ```sql
/// row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
/// ```
/// Which is not compatible with Spark. For Spark we rewrite AST to be
/// ```sql
/// row_number() OVER (ORDER BY monotonically_increasing_id())
/// ```
fn rewrite_row_number(statement: &mut ast::Statement) {
    let _ = visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Function(func) = expr {
            if func.name.to_string().to_lowercase() == "row_number" {
                if let Some(ast::WindowType::WindowSpec(ref mut window_spec)) = &mut func.over {
                    window_spec.window_frame = None;
                    window_spec.order_by = vec![ast::OrderByExpr {
                        expr: ast::Expr::Function(ast::Function {
                            name: ast::ObjectName::from(vec![ast::Ident::new(
                                "monotonically_increasing_id",
                            )]),
                            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                                duplicate_treatment: None,
                                args: vec![],
                                clauses: vec![],
                            }),
                            filter: None,
                            null_treatment: None,
                            over: None,
                            within_group: vec![],
                            uses_odbc_syntax: false,
                            parameters: ast::FunctionArguments::None,
                        }),
                        options: ast::OrderByOptions {
                            asc: None,
                            nulls_first: None,
                        },
                        with_fill: None,
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
    const SPECIAL_VALUES: &[&str] = &[
        "nan",
        "inf",
        "infinity",
        "+inf",
        "+infinity",
        "-inf",
        "-infinity",
    ];

    let _ = visit_expressions_mut(statement, |expr: &mut ast::Expr| {
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
                                }),
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

/// Rename `to_char` function calls to `date_format` for Spark compatibility
/// Spark <4 doesn't support formatting dates with `to_char` function
fn rewrite_date_format(statement: &mut ast::Statement) {
    let _ = visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Function(func) = expr {
            if func.name.to_string().to_lowercase() == "to_char" {
                func.name = ast::ObjectName::from(vec![ast::Ident::new("date_format")]);
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Timestamp is weird in Spark.
/// First of all, TIMESTAMP type is SQL in "naive", it doesn't have associated timezone. But in Spark it actually has.
/// And Spark doesn't support TIMESTAMP WITH TIME ZONE type, so we rewrite it to just TIMESTAMP.
/// Because of this we also rewrite calls to make_timestamptz into make_timestamp, dropping milliseconds argument,
/// as it's not supported by Spark.
fn rewrite_timestamps(statement: &mut ast::Statement) {
    let _ = visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Function(func) = expr {
            let func_name = func.name.to_string().to_lowercase();
            if func_name == "make_timestamptz" {
                func.name = ast::ObjectName::from(vec![ast::Ident::new("make_timestamp")]);

                // Remove milliseconds (not supported by Spark)
                if let ast::FunctionArguments::List(ref mut arg_list) = &mut func.args {
                    if arg_list.args.len() >= 7 {
                        arg_list.args.remove(6);
                    }
                }
            } else if func_name.starts_with("to_timestamp") {
                // Spark only has to_timestamp function, no to_timestamp_nanos, etc
                if func_name != "to_timestamp" {
                    func.name = ast::ObjectName::from(vec![ast::Ident::new("to_timestamp")]);
                }

                // Spark's `to_timestamp` supports passing only format, while DataFusion allows to
                // match list of Chrono patterns. So we remove ALL patterns from this func call
                if let ast::FunctionArguments::List(ref mut arg_list) = &mut func.args {
                    if arg_list.args.len() > 1 {
                        arg_list.args.truncate(1);
                    }
                }
            }
        } else if let ast::Expr::Cast { data_type, .. } = expr {
            // Rewrite TIMESTAMP WITH TIME ZONE to just TIMESTAMP
            if let ast::DataType::Timestamp(_, ast::TimezoneInfo::WithTimeZone) = data_type {
                *data_type = ast::DataType::Timestamp(None, ast::TimezoneInfo::None);
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Rewrite interval expressions to use full names instead of abbreviations for Spark compatibility
/// e.g. "1 MONS" -> "1 MONTHS", "2 MINS" -> "2 MINUTES"
fn rewrite_intervals(statement: &mut ast::Statement) {
    let _ = visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Interval(interval) = expr {
            if let ast::Expr::Value(value_with_span) = interval.value.as_ref() {
                if let ast::Value::SingleQuotedString(interval_str) = &value_with_span.value {
                    *interval.value = ast::Expr::Value(ast::ValueWithSpan {
                        value: ast::Value::SingleQuotedString(expand_interval_abbreviations(
                            interval_str,
                        )),
                        span: value_with_span.span.clone(),
                    });
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

lazy_static! {
    /// Precompiled regexes for expanding interval abbreviations
    static ref INTERVAL_ABBREV_REGEXES: Vec<(regex::Regex, &'static str)> = vec![
        (regex::Regex::new(r"\b(\d+)\s+MONS\b").unwrap(), "${1} MONTHS"),
        (regex::Regex::new(r"\b(\d+)\s+MON\b").unwrap(), "${1} MONTH"),
        (regex::Regex::new(r"\b(\d+)\s+MINS\b").unwrap(), "${1} MINUTES"),
        (regex::Regex::new(r"\b(\d+)\s+MIN\b").unwrap(), "${1} MINUTE"),
        (regex::Regex::new(r"\b(\d+)\s+SECS\b").unwrap(), "${1} SECONDS"),
        (regex::Regex::new(r"\b(\d+)\s+SEC\b").unwrap(), "${1} SECOND"),
        (regex::Regex::new(r"\b(\d+)\s+HRS\b").unwrap(), "${1} HOURS"),
        (regex::Regex::new(r"\b(\d+)\s+HR\b").unwrap(), "${1} HOUR"),
        (regex::Regex::new(r"\b(\d+)\s+YRS\b").unwrap(), "${1} YEARS"),
        (regex::Regex::new(r"\b(\d+)\s+YR\b").unwrap(), "${1} YEAR"),
    ];
}

/// Ensure nested IS NULL/IS NOT NULL predicates are parenthesized for Spark compatibility
/// e.g. `col IS NULL IS NULL` -> `(col IS NULL) IS NULL`
fn rewrite_nested_is_null(statement: &mut ast::Statement) {
    let _ = visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::IsNull(inner) | ast::Expr::IsNotNull(inner) = expr {
            if matches!(
                inner.as_ref(),
                ast::Expr::IsNull(_) | ast::Expr::IsNotNull(_)
            ) {
                let inner_clone = inner.as_ref().clone();
                *inner = Box::new(ast::Expr::Nested(Box::new(inner_clone)));
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Expand interval abbreviations to full names for Spark compatibility
fn expand_interval_abbreviations(interval_str: &str) -> String {
    let mut result = interval_str.to_string();
    for (re, replacement) in INTERVAL_ABBREV_REGEXES.iter() {
        result = re.replace_all(&result, *replacement).to_string();
    }
    result
}

/// DataFusion logical plan which uses compound names when selecting from subquery:
/// ```sql
/// SELECT orders.customer_name, orders.customer_age FROM (SELECT orders.customer_name, orders.customer_age FROM orders)
/// ```
/// This is not a valid SQL, as `orders` isn't available once we get out of first query.
/// So we rewrite logical plan to replace compound names with just the column names in projections
/// that select data from another projection
fn rewrite_subquery_column_identifiers(plan: LogicalPlan) -> Result<LogicalPlan> {
    let processed_plan = plan
        .transform_up_with_subqueries(|p| {
            if let LogicalPlan::Projection(projection) = &p {
                // only touch projections that read from another projection
                if matches!(*projection.input, LogicalPlan::Projection { .. }) {
                    let rewritten_exprs = projection
                        .expr
                        .iter()
                        .map(|e| {
                            e.clone()
                                .transform_up(|mut ex| {
                                    if let Expr::Column(c) = &mut ex {
                                        *c = Column::from_name(c.name.clone());
                                        Ok(Transformed::yes(ex))
                                    } else {
                                        Ok(Transformed::no(ex))
                                    }
                                })
                                .map(|t| t.data)
                        })
                        .collect::<std::result::Result<_, _>>()?;
                    let new_plan_node =
                        p.with_new_exprs(rewritten_exprs, vec![(*projection.input).clone()])?;
                    return Ok(Transformed::yes(new_plan_node));
                }
            }

            Ok(Transformed::no(p))
        })
        .map_err(|e| {
            VegaFusionError::vendor(format!(
                "Failed to rewrite subquery column identifiers: {}",
                e
            ))
        })?
        .data;

    Ok(processed_plan)
}

/// Rewrite datetime formatting expressions to be compatible with Spark
fn rewrite_datetime_formatting(plan: LogicalPlan) -> Result<LogicalPlan> {
    let processed_plan = plan
        .transform_up_with_subqueries(|p| {
            let p = p
                .map_expressions(|expr| {
                    expr.transform(&|e| {
                        if let Expr::ScalarFunction(sf) = &e {
                            if sf.name().eq_ignore_ascii_case("to_char") {
                                let mut new_args = sf.args.clone();
                                if new_args.len() > 1 {
                                    if let Expr::Literal(ScalarValue::Utf8(Some(format_str)), _) =
                                        &new_args[1]
                                    {
                                        let spark_format =
                                            chrono_to_spark(format_str).map_err(|e| {
                                                datafusion_common::DataFusionError::External(
                                                    Box::new(e),
                                                )
                                            })?;
                                        new_args[1] = Expr::Literal(
                                            ScalarValue::Utf8(Some(spark_format)),
                                            None,
                                        );
                                        let new_sf = ScalarFunction {
                                            func: sf.func.clone(),
                                            args: new_args,
                                        };
                                        return Ok(Transformed::yes(Expr::ScalarFunction(new_sf)));
                                    }
                                }
                            }
                        }
                        Ok(Transformed::no(e))
                    })
                })?
                .data;
            Ok(Transformed::yes(p))
        })
        .map_err(|e: datafusion_common::DataFusionError| {
            VegaFusionError::vendor(format!("Failed to rewrite datetime formatting: {}", e))
        })?
        .data;

    Ok(processed_plan)
}

lazy_static! {
    /// chrono-strftime → SparkSQL pattern map
    static ref CHRONO_SPARK_MAP: HashMap<&'static str, &'static str> = {
        HashMap::from([
            // year
            ("Y", "yyyy"), ("y", "yy"),
            // month
            ("m", "MM"), ("b", "MMM"), ("h", "MMM"), ("B", "MMMM"),
            // day
            ("d", "dd"), ("e", "d"), ("j", "DDD"),
            // hour / minute / second
            ("H", "HH"), ("I", "hh"), ("k", "H"), ("l", "h"),
            ("M", "mm"), ("S", "ss"),
            // week
            ("U", "ww"), ("W", "ww"), ("V", "ww"),
            // weekday names
            ("a", "EEE"), ("A", "EEEE"),
            // AM / PM
            ("p", "a"), ("P", "a"),
            // timezone
            ("z", "Z"), ("Z", "z"),
        ])
    };
}

/// Convert a chrono `strftime` pattern (e.g. "%Y-%m-%d %H:%M:%S")
/// to a Spark-SQL `date_format` pattern (e.g. "yyyy-MM-dd HH:mm:ss").
fn chrono_to_spark(fmt: &str) -> Result<String> {
    let mut out = String::with_capacity(fmt.len() * 2);
    let mut chars = fmt.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '%' {
            // Check if this character needs to be quoted
            // Common separators like -, :, space don't need quotes
            // Letters and other special characters do need quotes
            if !matches!(c, '-' | ':' | ' ' | '/' | ',' | '.') {
                // Collect consecutive literal characters that need quoting
                let mut literal = String::new();
                literal.push(c);

                // Continue collecting non-% characters that need quoting
                while let Some(&next_c) = chars.peek() {
                    if next_c == '%' || !matches!(next_c, '-' | ':' | ' ' | '/' | ',' | '.') {
                        break;
                    }
                    literal.push(chars.next().unwrap());
                }

                // Wrap the literal string in single quotes
                out.push_str(&format!("\\'{}\\'", &literal));
            } else {
                // Characters that don't need quoting (like -, :, space)
                out.push(c);
            }
            continue;
        }

        // literal %%
        if chars.peek() == Some(&'%') {
            out.push('%');
            chars.next();
            continue;
        }

        // collect every char up to and incl. the terminating alpha
        let mut modifier = String::new(); // '.', ':', '#' …
        let mut digits = String::new(); // width like 3 in %3f
        let mut letter = '\0';

        while let Some(&ch) = chars.peek() {
            chars.next();
            if ch.is_ascii_alphabetic() {
                letter = ch;
                break;
            } else if ch.is_ascii_digit() {
                digits.push(ch);
            } else {
                modifier.push(ch);
            }
        }

        match letter {
            // -------- fractional seconds --------
            'f' => {
                // width: %f        -> 9  (nanoseconds)
                //        %3f       -> 3  (fixed)
                //        %.f       -> 9  (leading dot)
                //        %.3f      -> 3  (leading dot, fixed)
                let width: usize = digits.parse::<usize>().unwrap_or(9).clamp(1, 9);
                if modifier.contains('.') {
                    out.push('.');
                }
                out.push_str(&"S".repeat(width)); // S, SS, … SSSSSSSSS
            }

            // -------- time-zone offsets --------
            'z' if modifier == ":" => out.push_str("XXX"), // %:z -> +09:30
            'z' if modifier == "::" => out.push_str("XXXXX"), // %::z -> +09:30:00
            'z' if modifier == ":::" => out.push_str("X"), // %:::z -> +09
            'z' => out.push_str("Z"),                      // %z  -> +0930

            // -------- everything else that has a direct map --------
            _ => {
                let key = &format!("{}{}", modifier, letter); // e.g. ""+"Y", ".f", ":z"
                match CHRONO_SPARK_MAP.get(key.as_str()) {
                    Some(rep) => out.push_str(rep),
                    None => {
                        return Err(VegaFusionError::vendor(format!(
                            "unsupported specifier %{}",
                            key
                        )))
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Rewrite column identifiers to properly quote column names with spaces or special characters
/// Spark SQL requires column names with spaces or special characters to be quoted with backticks
fn rewrite_column_identifiers(statement: &mut ast::Statement) {
    // Helper function to quote an identifier if needed
    let quote_if_needed = |ident: &mut ast::Ident| {
        if needs_quoting(&ident.value) {
            ident.quote_style = Some('`');
        }
    };

    // First handle expressions (column references)
    let _ = visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        match expr {
            ast::Expr::Identifier(ident) => {
                quote_if_needed(ident);
            }
            ast::Expr::CompoundIdentifier(identifiers) => {
                for ident in identifiers {
                    quote_if_needed(ident);
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });

    // Then handle aliases in all SELECT projections (including nested subqueries)
    quote_aliases_recursively(statement, &quote_if_needed);
}

/// Recursively traverse the AST to quote aliases in all SELECT statements
fn quote_aliases_recursively(
    statement: &mut ast::Statement,
    quote_if_needed: &impl Fn(&mut ast::Ident),
) {
    if let ast::Statement::Query(query) = statement {
        quote_aliases_in_query(query, quote_if_needed);
    }
}

/// Quote aliases in a query and all its nested subqueries
fn quote_aliases_in_query(query: &mut ast::Query, quote_if_needed: &impl Fn(&mut ast::Ident)) {
    quote_aliases_in_set_expr(&mut query.body, quote_if_needed);
}

/// Quote aliases in a set expression (SELECT, UNION, etc.)
fn quote_aliases_in_set_expr(
    set_expr: &mut ast::SetExpr,
    quote_if_needed: &impl Fn(&mut ast::Ident),
) {
    match set_expr {
        ast::SetExpr::Select(select) => {
            quote_aliases_in_select(select, quote_if_needed);
        }
        ast::SetExpr::Query(query) => {
            quote_aliases_in_query(query, quote_if_needed);
        }
        ast::SetExpr::SetOperation { left, right, .. } => {
            quote_aliases_in_set_expr(left, quote_if_needed);
            quote_aliases_in_set_expr(right, quote_if_needed);
        }
        _ => {
            // Other set expression types don't have aliases to quote
        }
    }
}

/// Quote aliases in a SELECT statement and handle nested subqueries
fn quote_aliases_in_select(select: &mut ast::Select, quote_if_needed: &impl Fn(&mut ast::Ident)) {
    // Quote aliases in the projection list
    for projection in &mut select.projection {
        if let ast::SelectItem::ExprWithAlias { alias, .. } = projection {
            quote_if_needed(alias);
        }
    }

    // Recursively handle subqueries in the FROM clause
    for table_with_joins in &mut select.from {
        quote_aliases_in_table_with_joins(table_with_joins, quote_if_needed);
    }
}

/// Quote aliases in table references and their joins
fn quote_aliases_in_table_with_joins(
    table_with_joins: &mut ast::TableWithJoins,
    quote_if_needed: &impl Fn(&mut ast::Ident),
) {
    quote_aliases_in_table_factor(&mut table_with_joins.relation, quote_if_needed);

    for join in &mut table_with_joins.joins {
        quote_aliases_in_table_factor(&mut join.relation, quote_if_needed);
    }
}

/// Quote aliases in table factors (tables, subqueries, etc.)
fn quote_aliases_in_table_factor(
    table_factor: &mut ast::TableFactor,
    quote_if_needed: &impl Fn(&mut ast::Ident),
) {
    match table_factor {
        ast::TableFactor::Derived { subquery, .. } => {
            quote_aliases_in_query(subquery, quote_if_needed);
        }
        ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            quote_aliases_in_table_with_joins(table_with_joins, quote_if_needed);
        }
        _ => {
            // Other table factor types don't have nested queries to process
        }
    }
}

/// Check if a column name needs to be quoted in Spark SQL
fn needs_quoting(name: &str) -> bool {
    // Don't quote function calls (contain parentheses)
    if name.contains('(') || name.contains(')') {
        return false;
    }

    // Check if the name contains spaces, hyphens, or starts with a digit
    name.contains(' ')
        || name.contains('-')
        || name.chars().next().map_or(false, |c| c.is_ascii_digit())
        || has_special_chars_needing_quotes(name)
        || is_sql_reserved_word(name)
}

/// Check if a name contains special characters that require quoting
/// This is more restrictive than the previous version to avoid quoting function calls
fn has_special_chars_needing_quotes(name: &str) -> bool {
    // Only quote for specific problematic characters, not all non-alphanumeric
    name.chars().any(|c| {
        matches!(
            c,
            '!' | '@'
                | '#'
                | '$'
                | '%'
                | '^'
                | '&'
                | '*'
                | '+'
                | '='
                | '['
                | ']'
                | '{'
                | '}'
                | '|'
                | '\\'
                | ':'
                | ';'
                | '"'
                | '\''
                | '<'
                | '>'
                | ','
                | '.'
                | '?'
                | '/'
                | '~'
                | '`'
        )
    })
}

lazy_static! {
    /// SQL reserved words that need quoting in Spark SQL
    /// This includes both ANSI SQL and Spark SQL specific reserved words
    static ref RESERVED_WORDS: std::collections::HashSet<&'static str> = {
        std::collections::HashSet::from([
            // ANSI SQL reserved words
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT",
            "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX",
            "VIEW", "DATABASE", "SCHEMA", "COLUMN", "CONSTRAINT", "PRIMARY", "KEY",
            "FOREIGN", "REFERENCES", "UNIQUE", "NOT", "NULL", "DEFAULT", "CHECK",
            "UNION", "INTERSECT", "EXCEPT", "JOIN", "INNER", "LEFT", "RIGHT", "FULL",
            "OUTER", "CROSS", "ON", "USING", "AS", "DISTINCT", "ALL", "ANY", "SOME",
            "EXISTS", "IN", "BETWEEN", "LIKE", "IS", "AND", "OR", "CASE", "WHEN",
            "THEN", "ELSE", "END", "IF", "CAST", "CONVERT", "SUBSTRING", "TRIM",
            "UPPER", "LOWER", "LENGTH", "CHAR", "VARCHAR", "TEXT", "INTEGER", "INT",
            "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL",
            "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "INTERVAL", "YEAR", "MONTH",
            "DAY", "HOUR", "MINUTE", "SECOND", "TRUE", "FALSE", "UNKNOWN",

            // Spark SQL specific reserved words
            "ARRAY", "MAP", "STRUCT", "BINARY", "TINYINT", "STRING", "PARTITION",
            "PARTITIONS", "CLUSTER", "DISTRIBUTE", "SORT", "TABLESAMPLE", "LATERAL",
            "WINDOW", "OVER", "ROW", "ROWS", "RANGE", "PRECEDING", "FOLLOWING",
            "UNBOUNDED", "CURRENT", "FIRST", "LAST", "IGNORE", "RESPECT", "NULLS",
            "ROLLUP", "CUBE", "GROUPING", "SETS", "PIVOT", "UNPIVOT", "TRANSFORM",
            "REDUCE", "AGGREGATE", "FILTER", "WITHIN", "COLLECT_LIST", "COLLECT_SET",
            "EXPLODE", "INLINE", "POSEXPLODE", "STACK", "JSON_TUPLE", "GET_JSON_OBJECT",
            "REGEXP_EXTRACT", "REGEXP_REPLACE", "SPLIT", "SIZE", "SORT_ARRAY",
            "ARRAY_CONTAINS", "MAP_KEYS", "MAP_VALUES", "NAMED_STRUCT", "STRUCT",
            "DESCRIBE", "DESC", "EXPLAIN", "ANALYZE", "CACHE", "UNCACHE", "REFRESH",
            "SHOW", "MSCK", "REPAIR", "RECOVER", "EXPORT", "IMPORT", "LOAD", "UNLOAD",
            "SET", "RESET", "ADD", "LIST", "WITH", "RECURSIVE", "TEMPORARY", "TEMP",
            "GLOBAL", "LOCAL", "LOCATION", "COMMENT", "TBLPROPERTIES", "SERDEPROPERTIES",
            "STORED", "INPUTFORMAT", "OUTPUTFORMAT", "SERDE", "DELIMITED", "FIELDS",
            "TERMINATED", "ESCAPED", "COLLECTION", "ITEMS", "KEYS", "LINES", "DEFINED",
            "RECORDREADER", "RECORDWRITER", "ROW_FORMAT", "STORED_AS", "CLUSTERED",
            "SORTED", "INTO", "BUCKETS", "SKEWED", "DIRECTORIES", "PURGE", "ARCHIVE",
            "UNARCHIVE", "TOUCH", "COMPACT", "CONCATENATE", "CHANGE", "REPLACE",
            "COLUMNS", "RLIKE", "REGEXP", "FUNCTION", "MACRO", "AGGREGATE", "RETURNS",
            "LANGUAGE", "DETERMINISTIC", "SQL", "CONTAINS", "READS", "MODIFIES",
            "NO", "CALLED", "INPUT", "SPECIFIC", "EXTERNAL", "SECURITY", "DEFINER",
            "INVOKER", "PATH", "ISOLATION", "LEVEL", "READ", "WRITE", "ONLY",
            "COMMITTED", "UNCOMMITTED", "REPEATABLE", "SERIALIZABLE", "WORK",
            "TRANSACTION", "START", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"
        ])
    };
}

/// Check if a name is a SQL reserved word that needs quoting in Spark SQL
fn is_sql_reserved_word(name: &str) -> bool {
    RESERVED_WORDS.contains(name.to_uppercase().as_str())
}
