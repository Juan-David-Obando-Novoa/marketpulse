/*
    The gold fact must have exactly one row per silver candle.

    fct_market_1m joins slv_ohlcv_1m to slv_fx_rates on a validity interval,
    and a range join against an interval table is precisely where a duplicated
    row appears if two intervals ever overlap. slv_fx_rates is built so that
    cannot happen and assert_no_overlapping_fx_intervals asserts it directly;
    this is the second line of defence, measured on the output rather than on
    the input, so it also catches an overlap introduced by the join predicate
    itself rather than by the data.

    Written as a singular test rather than dbt_utils.equal_rowcount for two
    reasons. The macro emits a synthetic join key that Trino cannot resolve,
    and -- more usefully -- it can only report that two totals disagree. This
    returns the offending instrument-minutes, so a failure is diagnosable from
    the stored failures table.
*/

with expected as (

    select exchange_id, symbol, bar_start, count(*) as n
    from {{ ref('slv_ohlcv_1m') }}
    group by 1, 2, 3

),

actual as (

    select symbol, bar_start, count(*) as n
    from {{ ref('fct_market_1m') }}
    group by 1, 2

)

select
    coalesce(e.symbol, a.symbol)        as symbol,
    coalesce(e.bar_start, a.bar_start)  as bar_start,
    e.n                                 as silver_rows,
    a.n                                 as gold_rows,
    case
        when a.n is null then 'candle missing from gold'
        when e.n is null then 'gold row with no silver candle'
        else 'range join multiplied the candle'
    end                                 as failure_reason
from expected as e
full outer join actual as a
    on e.symbol = a.symbol and e.bar_start = a.bar_start
where e.n is null or a.n is null or e.n <> a.n
