/// Graph-scale benchmarks for the Lunaris knowledge daemon.
///
/// These benchmarks measure the performance of the two-layer write architecture
/// (SQLite Write Store + Ladybug Query Store) at different data volumes.
///
/// Run with:
///   cargo bench --manifest-path knowledge/Cargo.toml
///
/// Results are stored in target/criterion/ and compared against the previous
/// run automatically. A regression of >10% in any benchmark is a signal to
/// investigate before merging.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use knowledge::db;
use knowledge::proto::Event;

fn make_event(id: &str, event_type: &str) -> Event {
    Event {
        id: id.to_string(),
        r#type: event_type.to_string(),
        timestamp: 1_000_000,
        source: "bench".to_string(),
        pid: 1,
        session_id: "bench-session".to_string(),
        payload: vec![],
    }
}

fn make_events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| make_event(&format!("id-{i:08}"), "file.opened"))
        .collect()
}

/// Benchmark SQLite batch writes at different batch sizes.
///
/// This measures the raw throughput of the Write Store. The key metric is
/// events/second. A regression here means the hot path that every eBPF event
/// goes through has slowed down.
fn bench_write_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("write_batch");

    for size in [10usize, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b, &size| {
                let events = make_events(size);
                b.iter(|| {
                    rt.block_on(async {
                        // Use in-memory SQLite for each iteration to avoid
                        // accumulation effects across iterations.
                        let pool = db::open(":memory:").await.unwrap();
                        db::write_batch(&pool, &events).await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

/// Benchmark SQLite write throughput with pre-warmed pool.
///
/// Simulates the steady-state where the pool is already open and we are
/// continuously writing batches. This is closer to the real workload.
fn bench_write_batch_steady_state(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let pool = rt.block_on(async { db::open(":memory:").await.unwrap() });

    let mut group = c.benchmark_group("write_batch_steady_state");

    for size in [10usize, 100, 1_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b, &size| {
                let events = make_events(size);
                b.iter(|| {
                    rt.block_on(async {
                        db::write_batch(&pool, &events).await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

/// Benchmark duplicate detection in write_batch.
///
/// INSERT OR IGNORE is the mechanism that handles duplicates. This measures
/// whether duplicate-heavy workloads (e.g. same file opened repeatedly) are
/// handled efficiently.
fn bench_write_batch_duplicates(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("write_batch_duplicates");

    for size in [100usize, 1_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b, &size| {
                // All events have the same ID - all are duplicates after the first.
                let events: Vec<Event> = (0..size)
                    .map(|_| make_event("duplicate-id", "file.opened"))
                    .collect();

                b.iter(|| {
                    rt.block_on(async {
                        let pool = db::open(":memory:").await.unwrap();
                        db::write_batch(&pool, &events).await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_write_batch,
    bench_write_batch_steady_state,
    bench_write_batch_duplicates,
);
criterion_main!(benches);
