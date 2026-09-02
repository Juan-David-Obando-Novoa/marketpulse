{% snapshot snap_instrument_status %}

{{
    config(
        target_schema='snapshots',
        unique_key='symbol',
        strategy='check',
        check_cols=['status', 'tick_size', 'step_size', 'min_notional'],
        invalidate_hard_deletes=True
    )
}}

/*
    Slowly-changing dimension over the venue's own instrument metadata.

    Venues change filters -- tick size, lot size, minimum notional -- without
    announcement, and a trade that looks invalid against today's tick size is
    almost always valid against the one in force when it printed. Without
    history, every such check produces false positives on old data forever.

    `check` strategy rather than `timestamp`: the venue publishes no
    modification timestamp, so the only honest change detector is comparing
    the values themselves. invalidate_hard_deletes is on so that a delisting
    closes the record instead of leaving it open forever.
*/

select
    symbol,
    base_asset,
    quote_asset,
    status,
    cast(tick_size as decimal(38, 18))    as tick_size,
    cast(step_size as decimal(38, 18))    as step_size,
    cast(min_notional as decimal(38, 18)) as min_notional,
    observed_at
from {{ source('bronze', 'instrument_metadata') }}

{% endsnapshot %}
