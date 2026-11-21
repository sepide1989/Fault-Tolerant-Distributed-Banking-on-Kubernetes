# Banking evidence

UTC: 2026-11-20T09:53:03.142704+00:00

Result: FAIL

- one_control_plane_two_workers: True
- all_nodes_ready: True
- bank-a.three_queryable_ready: True
- bank-a.one_primary_all_observed: True
- bank-a.local_and_remote_standby: True
- bank-a.strict_two_sync: True
- bank-a.streaming_and_sync: True
- bank-a.archiver_progress_after_last_error: True
- bank-a.independent_claims: True
- bank-a.completed_backup: True
- bank-b.three_queryable_ready: True
- bank-b.one_primary_all_observed: True
- bank-b.local_and_remote_standby: True
- bank-b.strict_two_sync: True
- bank-b.streaming_and_sync: True
- bank-b.archiver_progress_after_last_error: True
- bank-b.independent_claims: True
- bank-b.completed_backup: True
- bank-meta.three_queryable_ready: True
- bank-meta.one_primary_all_observed: True
- bank-meta.local_and_remote_standby: False
- bank-meta.strict_two_sync: True
- bank-meta.streaming_and_sync: True
- bank-meta.archiver_progress_after_last_error: True
- bank-meta.independent_claims: True
- bank-meta.completed_backup: True
- all_local_pv_affinities_match: True
- bank-a.application_spread: False
- bank-b.application_spread: False
- gateway.application_spread: True
- coordinator-recovery.application_spread: True
- cnpg_operator_survives_either_worker: True
- all_three_restore_drills_verified: False
- collection_complete: True
