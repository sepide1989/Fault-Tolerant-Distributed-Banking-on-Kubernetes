# Banking evidence

UTC: 2026-11-15T07:40:14.358106+00:00

Result: FAIL

- one_control_plane_two_workers: True
- all_nodes_ready: True
- bank-a.three_queryable_ready: False
- bank-a.one_primary_all_observed: False
- bank-a.local_and_remote_standby: False
- bank-a.strict_two_sync: True
- bank-a.streaming_and_sync: False
- bank-a.independent_claims: True
- bank-a.completed_backup: True
- bank-b.three_queryable_ready: True
- bank-b.one_primary_all_observed: True
- bank-b.local_and_remote_standby: True
- bank-b.strict_two_sync: True
- bank-b.streaming_and_sync: True
- bank-b.independent_claims: True
- bank-b.completed_backup: True
- bank-meta.three_queryable_ready: False
- bank-meta.one_primary_all_observed: False
- bank-meta.local_and_remote_standby: False
- bank-meta.strict_two_sync: True
- bank-meta.streaming_and_sync: False
- bank-meta.independent_claims: False
- bank-meta.completed_backup: True
- all_local_pv_affinities_match: False
- bank-a.application_spread: False
- bank-b.application_spread: True
- gateway.application_spread: True
- coordinator-recovery.application_spread: False
- all_three_restore_drills_verified: False
- collection_complete: True
