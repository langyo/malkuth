//! JSON-RPC throughput benchmarks.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use tokio::runtime::Runtime;

use malkuth::codec::take_frame;
use malkuth::{Client, Router, Server};
use malkuth::transport::TcpTransport;
use malkuth::Transport;
use serde_json::json;

fn bench_rpc_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let lis = rt.block_on(TcpTransport.listen("tcp://127.0.0.1:0")).unwrap();
    let addr = lis.local_addr().unwrap();
    let dial = format!("tcp://{addr}");

    let handler = Arc::new(
        Router::new().route("ping", |_| Box::pin(async { Ok(json!("pong")) })),
    );
    rt.spawn(async move {
        let _ = Server::serve_listener(lis, handler).await;
    });

    let mut group = c.benchmark_group("rpc_call_latency");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_call", |b| {
        b.to_async(&rt).iter(|| async {
            let mut c = Client::connect(&TcpTransport, &dial).await.unwrap();
            let r = black_box(c.call("ping", json!({})).await).unwrap();
            assert_eq!(r, json!("pong"));
        });
    });
    group.finish();

    // concurrent throughput: N clients each open a fresh connection and call once
    let mut group = c.benchmark_group("rpc_concurrent_throughput");
    for concurrency in [1usize, 4, 16, 64] {
        group.throughput(Throughput::Elements(concurrency as u64));
        group.bench_with_input(
            BenchmarkId::new("clients", concurrency),
            &concurrency,
            |b, &conc| {
                let dial = dial.clone();
                b.to_async(&rt).iter(|| {
                    let dial = dial.clone();
                    async move {
                        let mut tasks = Vec::with_capacity(conc);
                        for _ in 0..conc {
                            let d = dial.clone();
                            tasks.push(tokio::spawn(async move {
                                let mut c = Client::connect(&TcpTransport, &d).await.unwrap();
                                c.call("ping", json!({})).await.unwrap()
                            }));
                        }
                        for t in tasks {
                            black_box(t.await.unwrap());
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_codec_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_take_frame");

    let small = b"{\"jsonrpc\":\"2.0\",\"id\":1}\n".to_vec();
    group.throughput(Throughput::Bytes(small.len() as u64));
    group.bench_function("30B", |b| {
        b.iter(|| {
            let mut buf = small.clone();
            let _ = black_box(take_frame(&mut buf));
        });
    });

    let large_data = "X".repeat(4000);
    let large = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":1,\"params\":{{\"data\":\"{large_data}\"}}}}\n"
    )
    .into_bytes();
    group.throughput(Throughput::Bytes(large.len() as u64));
    group.bench_function("4KB", |b| {
        b.iter(|| {
            let mut buf = large.clone();
            let _ = black_box(take_frame(&mut buf));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_rpc_latency, bench_codec_framing);
criterion_main!(benches);
