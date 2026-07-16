# Continuous emission for demand-by-zone

DISP-2's 2-minute tumbling window (6-min grace) could emit either every incremental count update or only the final count once a window closes (via `suppress()`). We chose **continuous emission — no `suppress()`**.

DISP-4 (surge) joins against demand and must react to a burst *while it's happening* (e.g., a crowd leaving Bercy), not up to `window + grace` (8 min) after the fact — an 8-minute-delayed surge signal for a burst that resolves in under 2 minutes defeats the product goal. The accepted trade-off: `demand.by-zone` is noisier (multiple records per window per zone) in exchange for a surge signal that's actually timely. Downstream consumers of that topic must expect and handle repeated updates per window rather than a single authoritative count.
