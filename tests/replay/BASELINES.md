# Replay baseline provenance

The `*-baseline*.json` histories were recorded on 2026-09-09 before changing
engine behavior.
Only the recording cases and harness had been added at recording time.
The pinned Temporal CLI was 1.8.1 (embedded Server 1.31.2), with Python 3.12
and Temporal Python SDK 1.30.0 (also recorded in history SDK metadata).

`tests/beta_replay_cases.py` retains the exact definitions and history limit.
The recordings include workflow input/output contract activities, on-complete
archival, and every run of a history-triggered continuation chain. The live
scenario checks that the original step output survives continuation and each
business step executes once, in order. The low history threshold also causes
the contract case to continue; its entire chain is retained.

The original 14 histories are unchanged. Replay tests require all of these
representative families; they do not claim exhaustive interleaving coverage.
The append-only recorder refuses to overwrite existing baseline evidence.
Do not regenerate these histories to accommodate an engine behavior change.

Run the retained history gate with `make test-replay`. Run the live scenario
with `pytest tests/integration/test_beta_replay_baselines.py`.
