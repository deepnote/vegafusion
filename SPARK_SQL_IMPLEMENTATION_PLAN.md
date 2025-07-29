# Spark SQL Generation Implementation Plan

This document outlines the detailed implementation plan for adding Spark SQL generation capabilities to VegaFusion, building on the existing logical plan functionality.

## Overview

VegaFusion currently supports generating DataFusion logical plans from Vega specifications, but these plans need to be adapted for execution on Apache Spark clusters. This implementation adds a vendor-specific API that converts DataFusion logical plans to Spark-compatible SQL queries.

The main challenge is that DataFusion generates SQL that is not directly compatible with Spark due to differences in:
- Window function syntax
- Table reference handling in subqueries  
- Timestamp data types and timezone handling
- Special value representations (NaN, infinity)
- SQL dialect differences

## Architecture Overview

The implementation consists of several layers:

1. **Python API Layer**: `pre_transform_logical_plan_vendor()` - extends the existing public API with vendor-specific SQL generation
2. **Rust Bindings**: PyO3 bindings that bridge Python and Rust, processing logical plans and generating SQL
3. **SQL Generation Module**: Core logic in `vegafusion-runtime/src/sql/spark.rs` with functions for plan adaptation and SQL generation
4. **DataFusion Integration**: Uses DataFusion's unparser feature as the foundation for SQL generation

## Implementation Plan

### Phase 1: Core Infrastructure

#### 1.1 Create SQL Generation Module Structure

**Location**: `vegafusion-runtime/src/sql/`

The SQL generation functionality will be organized as a module with functions rather than structs:

- `vegafusion-runtime/src/sql/mod.rs` - module declaration and public exports
- `vegafusion-runtime/src/sql/spark.rs` - all Spark-specific SQL generation functions

**Dependencies to add**:
- Add `datafusion-sql = { version = "...", features = ["unparser"] }` to `vegafusion-runtime/Cargo.toml`

#### 1.2 Main SQL Generation Function

**Function**: `logical_plan_to_spark_sql(plan: &LogicalPlan) -> Result<String>`

**Problem it solves**: This is the main entry point that coordinates the entire process of converting a DataFusion logical plan into Spark-compatible SQL. DataFusion's native unparser generates SQL that contains several incompatibilities with Spark, so this function applies a three-step process:

1. **Plan Adaptation**: Transforms the logical plan nodes to be compatible with Spark's execution model
2. **SQL Generation**: Uses DataFusion's unparser to convert the adapted plan to base SQL  
3. **Post-processing**: Applies string-based transformations to fix remaining syntax differences

Without this coordinated approach, the generated SQL would fail to execute on Spark clusters due to various syntax and semantic incompatibilities.

### Phase 2: Logical Plan Adaptation for Spark

#### 2.1 Plan Rewriting Functions

The logical plan adaptation uses DataFusion's `TreeNodeRewriter` trait to systematically traverse and transform logical plan nodes. Each transformation addresses a specific incompatibility between DataFusion and Spark.

##### Window Function Fixes

**Function**: `rewrite_window_functions(window: &Window) -> Result<Transformed<LogicalPlan>>`

**Problem it solves**: DataFusion generates window functions with frame specifications that are incompatible with Spark. Specifically, `row_number()` functions are generated with `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` which is invalid syntax in Spark. Spark requires window functions to have proper `ORDER BY` clauses, and for `row_number()` specifically, it needs an ordering column (typically `monotonically_increasing_id()` for deterministic results).

This transformation identifies problematic window function expressions and rewrites them with Spark-compatible syntax, ensuring that window operations execute correctly on Spark clusters.

##### Table Reference Fixes

**Function**: `fix_table_references(expr: &Expr, input_plan: &LogicalPlan) -> Result<Expr>`

**Problem it solves**: DataFusion sometimes generates SQL with qualified column references (e.g., `table.column`) in contexts where Spark expects unqualified references. This commonly occurs in subqueries where DataFusion maintains table qualifiers that are not valid in the Spark execution context.

The function recursively traverses expressions to identify qualified column references and converts them to unqualified references when appropriate, preventing "column not found" errors during Spark execution.

##### Timestamp Handling

**Function**: `rewrite_timestamp_expressions(expr: &Expr) -> Result<Expr>`

**Problem it solves**: DataFusion uses `TIMESTAMP WITH TIME ZONE` data types which do not exist in Spark. Spark only supports `TIMESTAMP` (without timezone) and handles timezone information through session configuration (`SET TIME ZONE`). 

This transformation converts DataFusion's timezone-aware timestamps to Spark's timezone-naive timestamps, ensuring type compatibility between the systems.

#### 2.2 Special Value Handling

**Function**: `rewrite_special_values(expr: &Expr) -> Result<Expr>`

**Problem it solves**: DataFusion and Spark have different representations for special floating-point values:
- DataFusion represents NaN as the literal `NaN`
- DataFusion represents infinity as `inf` or `-inf`  
- Spark requires these to be expressed as function calls: `float('NaN')`, `float('inf')`, `float('-inf')`

Without this transformation, queries containing these special values would fail with parse errors in Spark. The function identifies literal expressions containing these special values and converts them to the appropriate Spark function call syntax.

### Phase 3: SQL Post-Processing

#### 3.1 String-based SQL Fixes

Even after logical plan transformations, some incompatibilities remain in the generated SQL that are easier to fix through string manipulation than plan rewriting.

**Function**: `post_process_spark_sql(sql: String) -> Result<String>`

**Problem it solves**: This function applies final transformations to the SQL string generated by DataFusion's unparser. Some syntax differences between DataFusion and Spark are easier to handle at the string level, particularly format patterns and edge cases that don't warrant complex plan transformations.

**Function**: `fix_datetime_patterns(sql: String) -> Result<String>`

**Problem it solves**: DataFusion and Spark use different format strings for datetime operations:
- DataFusion uses patterns like `YYYY-MM-DD`, `HH24`, `MI`
- Spark uses Java's SimpleDateFormat patterns like `yyyy-MM-dd`, `HH`, `mm`

This transformation ensures that any datetime formatting functions in the generated SQL use Spark-compatible format strings, preventing runtime errors when the queries are executed.

### Phase 4: Python API Implementation

#### 4.1 Vendor-specific Python API

**Location**: `vegafusion-python/vegafusion/runtime.py`

**Function**: `pre_transform_logical_plan_vendor()`

**Problem it solves**: The existing `pre_transform_logical_plan()` function returns DataFusion logical plans that are not directly executable on external systems like Spark. This vendor-specific API extends the functionality to generate SQL queries adapted for specific execution engines.

The function maintains the same interface as the public API but adds an `output_format` parameter to specify the target SQL dialect. This allows users to get Spark-compatible SQL without changing their existing VegaFusion workflow significantly.

#### 4.2 Rust Python Bindings

**Location**: `vegafusion-python/src/lib.rs`

**Function**: `pre_transform_logical_plan_vendor()` (PyO3 binding)

**Problem it solves**: This binding bridges the gap between Python and Rust, allowing Python users to access the Spark SQL generation functionality. It follows a two-step process:

1. **Reuse existing functionality**: Calls the existing `pre_transform_logical_plan()` to get logical plans
2. **Add vendor processing**: For each dataset containing a logical plan, converts it to the requested SQL dialect

**Function**: `process_datasets_for_spark()`

**Problem it solves**: The public API returns datasets with logical plans as Python objects, but these need to be converted back to Rust DataFusion plans, processed through the Spark SQL generator, and then enhanced with the generated SQL. This function handles the object marshaling and SQL generation, adding a `sparksql` key to each dataset dictionary that contains the Spark-compatible SQL query.

### Phase 5: Testing

#### 5.1 Unit Tests

**Location**: `vegafusion-runtime/src/sql/tests/`

**Purpose**: Validate that each transformation function correctly handles its specific compatibility issues. Each test creates a DataFusion logical plan with known incompatibilities and verifies that the generated Spark SQL contains the expected fixes.

Key test categories:
- **Window function tests**: Ensure `row_number()` and other window functions are properly rewritten
- **Table reference tests**: Verify qualified column references are fixed in subquery contexts  
- **Timestamp tests**: Confirm timezone-aware types are converted to Spark-compatible types
- **Special value tests**: Check that NaN/infinity literals are converted to function calls

### Phase 6: Error Handling

#### 6.1 Comprehensive Error Handling

**Error Type**: `SparkSqlError`

**Problem it solves**: The SQL generation process can fail at multiple stages (plan rewriting, DataFusion unparser, post-processing), and each failure mode requires different handling strategies. A comprehensive error type hierarchy allows:

- **Plan rewrite errors**: When logical plan transformations fail due to unsupported plan nodes or invalid transformations
- **Unparser errors**: When DataFusion's SQL generation fails, typically due to complex expressions or unsupported operations  
- **Post-processing errors**: When string-based transformations encounter unexpected SQL patterns
- **Unsupported operations**: When the input plan contains operations that cannot be expressed in Spark SQL

This structured error handling enables better diagnostics and recovery strategies for users.

The implementation targets Spark 3.3 specifically, providing a focused and well-tested solution for this widely-adopted Spark version.

## Dependencies

### New Crate Dependencies
- `datafusion-sql` with `unparser` feature (for base SQL generation)
- `regex` (for SQL post-processing) 