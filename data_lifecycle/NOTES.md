# Notes

- TASK.md has conflicting guidance for `get_summary` when a job is still running (return `RUNNING` summary vs raise `AzCopyNotFinishedError`). Implemented the `RUNNING` summary behavior per user direction.
