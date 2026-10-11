use criterion::{black_box, criterion_group, criterion_main, Criterion};
use concord::state_machine::StateMachine;
use concord::command::{ClientRequest, Command};

fn bench_put_throughput(c: &mut Criterion) {
    let mut sm = StateMachine::new();
    let mut index = 1;

    c.bench_function("state_machine_put", |b| {
        b.iter(|| {
            let req = ClientRequest {
                client_id: "bench_client".to_string(),
                sequence_number: index,
                command: Command::Put {
                    key: "perf/key".to_string(),
                    value: vec![0; 256], // 256 byte payload
                    lease_id: None,
                },
            };
            // black_box prevents the compiler from optimizing away the function call
            let _ = black_box(sm.apply(index, req));
            index += 1;
        })
    });
}

fn bench_get_throughput(c: &mut Criterion) {
    let mut sm = StateMachine::new();
    sm.apply(1, ClientRequest {
        client_id: "init".to_string(),
        sequence_number: 1,
        command: Command::Put {
            key: "perf/key".to_string(),
            value: vec![0; 256],
            lease_id: None,
        },
    }).unwrap();

    c.bench_function("state_machine_get", |b| {
        b.iter(|| {
            let _ = black_box(sm.get("perf/key"));
        })
    });
}

criterion_group!(benches, bench_put_throughput, bench_get_throughput);
criterion_main!(benches);