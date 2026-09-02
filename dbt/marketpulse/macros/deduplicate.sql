{#
    Deduplication by natural key (ADR-0007).

    The platform is at-least-once end to end, so every silver model that reads
    bronze has to collapse replays. Doing it with one macro rather than one
    window function per model means the tie-breaking rule is defined once: on a
    tie, keep the row that was ingested EARLIEST.

    That direction is deliberate and it is the opposite of the usual reflex.
    For an immutable fact -- a trade the venue has already printed -- a later
    copy is a replay, not a correction, and the first observation is the one
    with the honest ingestion timestamp. Keeping the latest would quietly
    inflate every ingestion-latency measurement by the length of the outage
    that caused the replay.
#}
{% macro deduplicate(relation, partition_by, order_by='_ingested_at asc') %}

with ranked as (

    select
        *,
        row_number() over (
            partition by {{ partition_by }}
            order by {{ order_by }}
        ) as _row_number
    from {{ relation }}

)

select * from ranked where _row_number = 1

{% endmacro %}


{#
    The incremental predicate every silver model shares.

    Two things it gets right that a hand-written `where ts > (select max(ts))`
    does not: it reconsiders a lookback window rather than only strictly newer
    rows, so late-arriving data is picked up; and it degrades to a full scan on
    a first run instead of comparing against a table that does not exist yet.
#}
{% macro incremental_window(timestamp_column, lookback_hours=none) %}
    {%- set hours = lookback_hours or var('incremental_lookback_hours', 24) -%}
    {% if is_incremental() %}
        {{ timestamp_column }} >= (
            select coalesce(max({{ timestamp_column }}), timestamp '1970-01-01 00:00:00')
                   - interval '{{ hours }}' hour
            from {{ this }}
        )
    {% else %}
        true
    {% endif %}
{% endmacro %}
