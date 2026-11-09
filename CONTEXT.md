# Patient event ingestion

The service accepts patient events and reconstructs their changing state.

## Language

**Event**: A uniquely identified statement about a patient at a particular time.

**Acceptance**: Durable recording of an event for eventual processing.

**Result**: The immutable outcome of processing one event independently of patient state.

**Patient state**: The ordered reduction of a patient's completed results.

**Late event**: An event whose chronological position precedes an already applied result.

**Replay**: Rebuilding patient state from retained results in chronological order.

**Pending work**: Accepted events without committed results, including failed events.

**Blocked patient**: A patient whose processing is paused because an event exhausted its retries.
