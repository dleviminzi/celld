// This public integration test owns a real Tokio/V8 host rather than the
// deterministic core execution domain; its outer deadlines must use wall time.
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use celld::js::{Engine, Worker, WorkerConfig, WorkerConfigOptions};
use celld::pool::Pool;
use celld_logic::isolate::PoolLimits;

fn pool(source: &str) -> Pool {
    let config = Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: source.into(),
        script_name: "retirement-test".into(),
        do_classes: if source.contains("export class C") {
            vec!["C".into()]
        } else {
            vec![]
        },
        bindings: vec![],
        r2_bindings: vec![],
        d1_bindings: vec![],
        kv_bindings: vec![],
        queue_bindings: vec![],
        queue_consumers: vec![],
        workflow_bindings: vec![],
        vars: vec![],
        node: "test".into(),
        modules: vec![],
        compat: Default::default(),
    }));
    Pool::new(
        PoolLimits {
            grow_at: 2,
            shrink_under: 1,
            max_stateless: 1,
            max_requests: Some(8),
            max_cells: 8,
        },
        Duration::from_secs(1),
        Box::new(move || Worker::load_config(config.clone())),
    )
}

// This binary owns its environment; no other tests run alongside it. Exercise
// real V8 entries, native timer ops, request affiliations and pool reclamation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_background_requests_release_their_isolates() {
    std::env::set_var("CELLD_DEPLOY_MAX_AGE_S", "0");
    Engine::init();
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    for (grace, request_id) in [(0, None), (1, Some(celld::js::next_request_id()))] {
        std::env::set_var("CELLD_DEPLOY_MAX_AGE_S", grace.to_string());
        let pool = pool(
            r#"
            export default {
                fetch(request, env, ctx) {
                    ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 60000)));
                    return new Response('accepted');
                }
            };
        "#,
        );
        let admission = pool.admit(false).expect("admit");
        let slot = admission.slot().clone();
        let (reply, receive) = tokio::sync::oneshot::channel();
        let driver = tokio::spawn(celld::runtime::drive(
            slot,
            celld::WorkerJob::Fetch {
                queued_at: Instant::now(),
                entrypoint: None,
                invocation_limits: None,
                url: "http://test/".into(),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: vec![],
                request_id,
                tail_report: None,
                reply,
            },
            None,
        ));
        drop(admission);
        let response = tokio::time::timeout(Duration::from_secs(5), receive)
            .await
            .expect("response deadline")
            .expect("response channel")
            .expect("response");
        assert_eq!(response.status, 200);
        assert_eq!(
            pool.census().requests,
            1,
            "waitUntil pins the heap after response"
        );
        assert!(
            !driver.is_finished(),
            "current-generation background work must survive its response"
        );

        pool.retire_all();
        if grace > 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            assert!(
                !driver.is_finished(),
                "retirement must honor the grace period"
            );
            // Repeating retirement cannot restart or extend the first deadline.
            std::env::set_var("CELLD_DEPLOY_MAX_AGE_S", "60");
            pool.retire_all();
        }
        tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .expect("retired request must finish even after its handler answered")
            .expect("driver did not panic");
        assert_eq!(pool.census().requests, 0);
        pool.reap_empty();
        assert!(
            pool.is_drained(),
            "the old heap must be released, not just hidden from state"
        );
    }
    std::env::set_var("CELLD_DEPLOY_MAX_AGE_S", "0");
    for gated in [false, true] {
        let source = if gated {
            r#"export class C {
                constructor(state) {
                    state.blockConcurrencyWhile(() => new Promise(resolve => setTimeout(resolve, 60000)));
                }
                fetch() { return new Response('must not run before gate opens'); }
            }
            export default { fetch() { return new Response('worker'); } };"#
        } else {
            r#"export class C {
                constructor(state) { this.state = state; }
                fetch() {
                    this.state.waitUntil(new Promise(resolve => setTimeout(resolve, 60000)));
                    return new Response('accepted');
                }
            }
            export default { fetch() { return new Response('worker'); } };"#
        };
        let pool = pool(source);
        let admission = pool.admit(false).expect("cell admission");
        let slot = admission.slot().clone();
        let scope = format!("C:retirement-{gated}");
        slot.turn(|worker| worker.own_cell(&scope, None))
            .await
            .expect("own cell");
        let (reply, mut receive) = tokio::sync::oneshot::channel();
        let driver = tokio::spawn(celld::runtime::drive_cell(
            admission,
            celld::js::CellJob::Fetch {
                request_id: None,
                scope,
                name: None,
                url: "http://test/".into(),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: vec![],
                reply,
                order: None,
            },
            None,
            None,
        ));
        if gated {
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut receive)
                    .await
                    .is_err()
            );
        } else {
            let response = tokio::time::timeout(Duration::from_secs(5), &mut receive)
                .await
                .expect("cell response deadline")
                .expect("cell response channel")
                .expect("cell response");
            assert_eq!(response.status, 200);
        }
        assert_eq!(pool.census().requests, 1);
        pool.retire_all();
        tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .expect("retired cell request must release its affiliation")
            .expect("cell driver");
        if gated {
            assert!(receive
                .await
                .expect("queued request gets an explicit failure")
                .is_err());
        }
        assert_eq!(pool.census().requests, 0);
        pool.reap_empty();
        assert!(pool.is_drained());
    }
}
