/*
    Every trade price must sit on the venue's price grid.

    A price off the tick grid is not a market event; it is a parsing bug, a
    unit error, or a decimal that went through a float somewhere. This is the
    cheapest possible end-to-end assertion that the decimal handling described
    in ADR-0006 actually held all the way from the websocket to the warehouse.

    Only instruments in the reference seed are checked, and only recent data,
    because tick sizes change and the historical grid is the snapshot's job.
*/

with recent_trades as (

    select t.symbol, t.trade_id, t.price, t.event_time
    from {{ ref('slv_trades') }} as t
    where t.event_time >= current_timestamp - interval '2' day

)

select
    t.symbol,
    t.trade_id,
    t.price,
    i.tick_size,
    mod(cast(t.price / i.tick_size as decimal(38, 6)), 1) as grid_remainder
from recent_trades as t
inner join {{ ref('instrument_reference') }} as i
    on t.symbol = i.symbol
where mod(cast(t.price / i.tick_size as decimal(38, 6)), 1) <> 0
