use lazy_static::lazy_static;
use tokio::runtime::Runtime;
use vegafusion_runtime::tokio_runtime::TOKIO_THREAD_STACK_SIZE;

lazy_static! {
    static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
        .build()
        .unwrap();
}

#[cfg(test)]
mod test_timestamp_parsing {
    use crate::TOKIO_RUNTIME;
    use rstest::rstest;
    use serde_json::json;
    use vegafusion_core::proto::gen::pretransform::PreTransformSpecOpts;
    use vegafusion_core::runtime::VegaFusionRuntimeTrait;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

    /// Run `toDate` over a single string value and return the stringified result.
    async fn to_date(input: &str, local_tz: &str, default_input_tz: &str) -> String {
        let spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [{
                "name": "source",
                "values": [{"ts": input}],
                "transform": [
                    {"type": "formula", "expr": "toDate(datum.ts)", "as": "parsed"}
                ]
            }],
            "marks": [{
                "type": "symbol",
                "from": {"data": "source"},
                "encode": {"update": {"x": {"field": "parsed"}}}
            }]
        }))
        .unwrap();

        let runtime = VegaFusionRuntime::default();
        let (spec, _warnings) = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    local_tz: local_tz.to_string(),
                    default_input_tz: Some(default_input_tz.to_string()),
                    keep_variables: vec![],
                    row_limit: None,
                    preserve_interactivity: true,
                },
            )
            .await
            .unwrap();

        let data = spec
            .data
            .iter()
            .find(|d| d.name == "source")
            .expect("source dataset");
        let values = data.values.as_ref().expect("inlined values");
        values.as_array().expect("array")[0]
            .as_object()
            .expect("object")
            .get("parsed")
            .expect("parsed field")
            .as_str()
            .expect("stringified datetime")
            .to_string()
    }

    /// Timestamps without a timezone suffix are parsed against an explicit list of formats,
    /// which must accept any fractional-second precision. Microsecond precision is what
    /// pandas, CSV and JSON round-trips emit by default.
    #[rstest(
        input,
        expected,
        case("2024-04-17T23:18:06", "2024-04-17T23:18:06.000"),
        case("2024-04-17T23:18:06.5", "2024-04-17T23:18:06.500"),
        case("2024-04-17T23:18:06.527", "2024-04-17T23:18:06.527"),
        case("2024-04-17T23:18:06.527738", "2024-04-17T23:18:06.527"),
        case("2024-04-17T23:18:06.527738912", "2024-04-17T23:18:06.527"),
        case("2024-04-17 23:18:06.527738", "2024-04-17T23:18:06.527")
    )]
    fn test_naive_fractional_seconds(input: &str, expected: &str) {
        assert_eq!(
            TOKIO_RUNTIME.block_on(to_date(input, "UTC", "UTC")),
            expected
        );
    }

    /// Timestamps carrying an offset take a separate parsing path, which must keep
    /// accepting arbitrary precision too.
    #[rstest(
        input,
        expected,
        case("2024-04-17T23:18:06.527738Z", "2024-04-17T23:18:06.527"),
        case("2024-04-17T23:18:06.527738+02:00", "2024-04-17T21:18:06.527")
    )]
    fn test_offset_fractional_seconds(input: &str, expected: &str) {
        assert_eq!(
            TOKIO_RUNTIME.block_on(to_date(input, "UTC", "UTC")),
            expected
        );
    }

    /// Following the browser: a timestamp without an offset is read in `default_input_tz`,
    /// one with an offset is an absolute instant, and a bare ISO date is always UTC.
    #[rstest(
        input,
        expected,
        case("2024-04-17T23:18:06.527738", "2024-04-18T03:18:06.527"),
        case("2024-04-17T23:18:06.527738Z", "2024-04-17T23:18:06.527"),
        case("2024-04-17T23:18:06.527738+02:00", "2024-04-17T21:18:06.527"),
        case("2024-04-17", "2024-04-17T00:00:00.000")
    )]
    fn test_default_input_tz(input: &str, expected: &str) {
        assert_eq!(
            TOKIO_RUNTIME.block_on(to_date(input, "UTC", "America/New_York")),
            expected
        );
    }

    /// The non-ISO formats in the parse list stay reachable.
    #[rstest(
        input,
        expected,
        case("April 17, 2024 23:18", "2024-04-17T23:18:00.000"),
        case("04/17/2024 23:18:06", "2024-04-17T23:18:06.000"),
        case("2024/04/17 23:18:06", "2024-04-17T23:18:06.000"),
        case("17 Apr 2024 23:18:06", "2024-04-17T23:18:06.000")
    )]
    fn test_non_iso_formats(input: &str, expected: &str) {
        assert_eq!(
            TOKIO_RUNTIME.block_on(to_date(input, "UTC", "UTC")),
            expected
        );
    }
}
