//! Fault-model entries for built-in merge laws (IVM-0, DESIGN.md §17.4).
//!
//! Every merge law registered in v0.5+ must have a named entry here that
//! describes which failure mode the law must survive during simulation.
//! This registry is the audit trail for law-level simulation discipline.

use crate::fault_model::{FaultCategory, FaultEntry, FaultModel};

/// Register all built-in law fault entries into a `FaultModel`.
///
/// Call this once at simulation startup via `rockstream_sim::with_builtins()`.
pub fn register_law_faults(model: &mut FaultModel) {
    // WeightAdd/v1 — Z-set weight addition (abelian group)
    model.register(FaultEntry {
        id: "law.weight_add.reorder",
        description: "WeightAdd/v1: operand pairs arrive out of order across epoch boundaries. \
                       The abelian group property guarantees commutativity, so the final merged \
                       value must be identical regardless of delivery order.",
        category: FaultCategory::Network,
    });
    model.register(FaultEntry {
        id: "law.weight_add.crash_replay",
        description: "WeightAdd/v1: the process crashes mid-WriteBatch after some merge \
                       operations are persisted. On restart the shard replays from its \
                       persisted frontier; the merged weight must be bit-identical to an \
                       uninterrupted run.",
        category: FaultCategory::Io,
    });
    model.register(FaultEntry {
        id: "law.weight_add.fence",
        description: "WeightAdd/v1: a storage fence is injected between the merge write \
                       and the frontier update. The shard must not expose uncommitted weight \
                       state via DbReader snapshot reads.",
        category: FaultCategory::Io,
    });
}

/// Fault-model entries registered by `register_law_faults`.
pub const LAW_FAULT_IDS: &[&str] = &[
    "law.weight_add.reorder",
    "law.weight_add.crash_replay",
    "law.weight_add.fence",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault_model::FaultModel;

    #[test]
    fn law_faults_register_without_collision() {
        let mut model = FaultModel::new();
        register_law_faults(&mut model);
        assert_eq!(model.len(), LAW_FAULT_IDS.len());
        for id in LAW_FAULT_IDS {
            assert!(model.get(id).is_some(), "missing fault entry: {id}");
        }
    }

    #[test]
    fn law_faults_are_categorized() {
        let mut model = FaultModel::new();
        register_law_faults(&mut model);
        // All three WeightAdd faults must be present and categorized
        assert_eq!(
            model.get("law.weight_add.reorder").unwrap().category,
            FaultCategory::Network
        );
        assert_eq!(
            model.get("law.weight_add.crash_replay").unwrap().category,
            FaultCategory::Io
        );
        assert_eq!(
            model.get("law.weight_add.fence").unwrap().category,
            FaultCategory::Io
        );
    }
}
