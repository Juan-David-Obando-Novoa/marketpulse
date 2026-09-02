{#
    Finance primitives.

    These exist so that a formula appears once. Spread in basis points is
    written four different ways across a typical analytics codebase -- against
    the mid, against the bid, times 100 instead of 10000 -- and the numbers
    disagree by enough to matter but not enough to notice.
#}

{% macro mid_price(bid, ask) %}
    (({{ bid }} + {{ ask }}) / 2)
{% endmacro %}


{#  Quoted spread relative to the mid, in basis points. The mid is the
    denominator (not the bid) so the measure is symmetric.  #}
{% macro spread_bps(bid, ask) %}
    case
        when {{ bid }} > 0 and {{ ask }} > {{ bid }}
        then (({{ ask }} - {{ bid }}) / {{ mid_price(bid, ask) }}) * 10000
    end
{% endmacro %}


{#  Volume-weighted average price. Guards the zero-volume window, which occurs
    for illiquid symbols and would otherwise divide by zero.  #}
{% macro vwap(price_column, quantity_column) %}
    case
        when sum({{ quantity_column }}) > 0
        then sum({{ price_column }} * {{ quantity_column }}) / sum({{ quantity_column }})
    end
{% endmacro %}


{#
    Order-flow imbalance: signed taker volume over total volume, in [-1, 1].

    +1 means every trade in the window lifted the offer, -1 that every trade
    hit the bid. This is the single most informative field the trade tape
    carries beyond price, and it exists only because buyer_is_maker was
    preserved from the venue.
#}
{% macro order_flow_imbalance(quantity_column, buyer_is_maker_column) %}
    case
        when sum({{ quantity_column }}) > 0
        then (
            sum(case when not {{ buyer_is_maker_column }} then {{ quantity_column }} else 0 end)
            - sum(case when {{ buyer_is_maker_column }} then {{ quantity_column }} else 0 end)
        ) / sum({{ quantity_column }})
    end
{% endmacro %}


{#  Realised volatility from log returns, annualised for the given bar length.
    365 days rather than 252: crypto does not observe a trading calendar.  #}
{% macro annualised_volatility(log_return_column, bar_minutes=1) %}
    (
        stddev_samp({{ log_return_column }})
        * sqrt({{ (365 * 24 * 60) // bar_minutes }})
    )
{% endmacro %}


{#  Floor a timestamp to a candle boundary. date_trunc cannot express
    'every five minutes', so the arithmetic is explicit and engine-independent. #}
{% macro floor_to_bar(timestamp_column, minutes=1) %}
    from_unixtime(
        floor(to_unixtime({{ timestamp_column }}) / {{ minutes * 60 }}) * {{ minutes * 60 }}
    )
{% endmacro %}
