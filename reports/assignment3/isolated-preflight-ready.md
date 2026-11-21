# Banking evidence

UTC: 2026-11-15T12:16:05.262987+00:00

Result: PASS

- one_control_plane_two_workers: True
- all_nodes_ready: True
- bank-a.three_queryable_ready: True
- bank-a.one_primary_all_observed: True
- bank-a.local_and_remote_standby: True
- bank-a.strict_two_sync: True
- bank-a.streaming_and_sync: True
- bank-a.independent_claims: True
- bank-a.completed_backup: True
- bank-b.three_queryable_ready: True
- bank-b.one_primary_all_observed: True
- bank-b.local_and_remote_standby: True
- bank-b.strict_two_sync: True
- bank-b.streaming_and_sync: True
- bank-b.independent_claims: True
- bank-b.completed_backup: True
- bank-meta.three_queryable_ready: True
- bank-meta.one_primary_all_observed: True
- bank-meta.local_and_remote_standby: True
- bank-meta.strict_two_sync: True
- bank-meta.streaming_and_sync: True
- bank-meta.independent_claims: True
- bank-meta.completed_backup: True
- all_local_pv_affinities_match: True
- bank-a.application_spread: True
- bank-b.application_spread: True
- gateway.application_spread: True
- coordinator-recovery.application_spread: True
- cnpg_operator_survives_either_worker: True
- all_three_restore_drills_verified: True
- collection_complete: True
