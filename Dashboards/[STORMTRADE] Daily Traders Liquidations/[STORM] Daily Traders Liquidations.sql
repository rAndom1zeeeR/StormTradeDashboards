WITH
    liquidation_orders AS (
        SELECT
            block_date,
            trader_addr,
            vault_token,
            CASE
                WHEN order_direction = 0 THEN 'LONG'
                WHEN order_direction = 1 THEN 'SHORT'
                ELSE 'UNKNOWN'
            END AS position_type,
            position_margin
        FROM
            stormtrade_ton.execute_order
        WHERE
            order_type = 'stop_loss_order' -- Liquidations happen through stop_loss_order
            AND block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
    ),
    daily_summary AS (
        SELECT
            block_date,
            vault_token,
            position_type,
            COUNT(*) AS liquidation_count,
            COUNT(DISTINCT trader_addr) AS unique_traders_liquidated
        FROM
            liquidation_orders
        GROUP BY
            block_date,
            vault_token,
            position_type
    )
SELECT
    ds.block_date,
    ds.vault_token,
    ds.position_type,
    ds.liquidation_count,
    ds.unique_traders_liquidated
FROM
    daily_summary ds
ORDER BY
    ds.block_date DESC,
    ds.vault_token,
    ds.position_type;