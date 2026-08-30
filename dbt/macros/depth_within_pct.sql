{#
  SQL port of streaming/depth.py's depth_within_pct (verified against real
  order-book data that rgCompactBuyOrders/rgCompactSellOrders entries are per-price-level
  quantities, NOT cumulative — see depth.py's `_pairs` docstring). Sums quantity across
  every price level within `pct` of `mid_expr`, not a max/cumulative lookup.

  compact_col: a flat BIGINT[] column, [price, qty, price, qty, ...].
  mid_expr: a SQL expression for the mid price (correlated to the same row).
  pct: a decimal fraction, e.g. 0.01 for 1%.
  side: 'buy' (prices >= mid*(1-pct)) or 'sell' (prices <= mid*(1+pct)).
#}
{% macro depth_within_pct(compact_col, mid_expr, pct, side) %}
    (
        select sum(pair.qty)
        from (
            select
                {{ compact_col }}[i] as price,
                {{ compact_col }}[i + 1] as qty
            from generate_series(1, len({{ compact_col }}), 2) as g(i)
        ) as pair
        where
            {% if side == 'buy' %}
            pair.price >= ({{ mid_expr }}) * (1 - {{ pct }})
            {% elif side == 'sell' %}
            pair.price <= ({{ mid_expr }}) * (1 + {{ pct }})
            {% else %}
            {{ exceptions.raise_compiler_error("side must be 'buy' or 'sell', got: " ~ side) }}
            {% endif %}
    )
{% endmacro %}
