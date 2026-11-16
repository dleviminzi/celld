use celld_logic::drain::{fleet_status, DrainToken, FleetUnsettled, RestorationBaseline};
use celld_logic::CapacityPeer;

const NOW: u64 = 100_000;

fn peer(node: &str, resident_cells: usize, headroom: bool) -> CapacityPeer {
    CapacityPeer {
        node: node.into(),
        addr: format!("{node}:8081"),
        expires_ms: NOW + 30_000,
        peer_protocol: 1,
        sampled_ms: NOW,
        owned_cells: Some(500),
        placement_weight: Some(1),
        bucket_format: Some(1),
        resident_cells,
        host_websockets: 0,
        rss_bytes: 0,
        in_use_bytes: None,
        pressured: false,
        memory_headroom: Some(headroom),
        restoring: 0,
        paced_handoff: true,
        rebalance_paused: false,
        draining: false,
    }
}

#[test]
fn empty_successor_unblocks_incumbents_between_memory_watermarks() {
    // Ownership includes dormant cells and is already balanced. Rebalancing
    // those counts cannot resolve this rollout's resident-memory imbalance.
    let peers = [
        peer("old-a", 300, false),
        peer("old-b", 350, false),
        peer("new", 0, true),
    ];
    assert_eq!(fleet_status(None, "new", NOW, &peers, 8), Ok(()));
}

#[test]
fn actual_pressure_still_blocks_the_next_donor() {
    let mut peers = [peer("old", 300, false), peer("new", 0, true)];
    peers[0].pressured = true;
    assert_eq!(
        fleet_status(None, "new", NOW, &peers, 8),
        Err(FleetUnsettled::MemoryHeadroom { node: "old".into() })
    );
}

#[test]
fn successor_must_report_usable_capacity() {
    let original = [peer("old", 300, false), peer("new", 0, true)];
    for case in 0..12 {
        let mut peers = original.clone();
        match case {
            0 => peers[1].memory_headroom = Some(false),
            1 => peers[1].memory_headroom = None,
            2 => peers[1].pressured = true,
            3 => peers[1].draining = true,
            4 => peers[1].paced_handoff = false,
            5 => peers[1].resident_cells = 300,
            6 => peers[1].sampled_ms = 0,
            7 => peers[1].sampled_ms = NOW - 30_001,
            8 => peers[1].sampled_ms = NOW + 1,
            9 => peers[1].restoring = 1,
            10 => peers[0].paced_handoff = false,
            11 => peers[0].draining = true,
            _ => unreachable!(),
        }
        assert!(
            matches!(
                fleet_status(None, "new", NOW, &peers, 8),
                Err(FleetUnsettled::MemoryHeadroom { .. })
            ),
            "case {case}"
        );
    }
}

#[test]
fn active_donor_and_recovery_backlog_remain_blocking() {
    let mut peers = [peer("old", 300, false), peer("new", 0, true)];
    let mut token = DrainToken {
        node: "previous-donor".into(),
        expires_ms: NOW + 1,
        restoration_baseline: vec![RestorationBaseline {
            node: "old".into(),
            restoring: 4,
        }],
    };
    assert_eq!(
        fleet_status(Some(&token), "new", NOW, &peers, 8),
        Err(FleetUnsettled::Drain)
    );
    token.expires_ms = NOW;
    peers[0].restoring = 13;
    assert_eq!(
        fleet_status(Some(&token), "new", NOW, &peers, 8),
        Err(FleetUnsettled::Restoration {
            current: 13,
            maximum: 12
        })
    );
    peers[0].restoring = 12;
    assert_eq!(fleet_status(Some(&token), "new", NOW, &peers, 8), Ok(()));
}

#[test]
fn expired_or_unpublished_successor_cannot_open_gate() {
    let mut peers = [peer("old", 300, false), peer("new", 0, true)];
    peers[1].expires_ms = NOW;
    assert_eq!(
        fleet_status(None, "new", NOW, &peers, 8),
        Err(FleetUnsettled::SelfUnpublished)
    );
}

#[test]
fn expired_incumbent_does_not_block_healthy_fleet() {
    let mut peers = [peer("old", 300, false), peer("new", 0, true)];
    peers[0].expires_ms = NOW;
    assert_eq!(fleet_status(None, "new", NOW, &peers, 8), Ok(()));
}

#[test]
fn healthy_single_node_still_becomes_ready() {
    assert_eq!(
        fleet_status(None, "new", NOW, &[peer("new", 0, true)], 8),
        Ok(())
    );
}
