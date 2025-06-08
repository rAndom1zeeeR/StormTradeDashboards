-- [STORM] PnL by address - Bar Chart
WITH
	-- Pre-filter by date range to reduce data processing
	date_range AS (
		SELECT
			CURRENT_DATE - INTERVAL '{{days}}' day AS start_date
	),
	-- Base tables with early date filtering
	storm_update_position AS (
		SELECT
			tx_hash,
			tx_lt,
			tx_now,
			user_position,
			direction,
			origin_op,
			position_size,
			position_open_notional,
			position_fee,
			quote_asset_weight,
			quote_asset_reserve,
			base_asset_reserve
		FROM
			stormtrade_ton.update_position
		WHERE
			block_date >= (
				SELECT
					start_date
				FROM
					date_range
			)
	),
	storm_complete_order AS (
		SELECT
			tx_hash,
			tx_lt,
			tx_now,
			user_position,
			direction,
			order_type,
			origin_op,
			position_size,
			position_open_notional,
			position_fee,
			quote_asset_weight,
			quote_asset_reserve,
			base_asset_reserve
		FROM
			stormtrade_ton.complete_order
		WHERE
			CAST(FROM_UNIXTIME (tx_now) AS DATE) >= (
				SELECT
					start_date
				FROM
					date_range
			)
	),
	-- Simplified relationship between positions and traders
	positions AS (
		SELECT DISTINCT
			trader_addr,
			user_position
		FROM
			(
				-- Get positions from trade_notification directly
				SELECT DISTINCT
					notification.trader_addr,
					update_position.user_position
				FROM
					storm_update_position AS update_position
					JOIN stormtrade_ton.trade_notification AS notification ON notification.tx_hash = update_position.tx_hash
				WHERE
					update_position.origin_op != 3427973859
				UNION
				-- Get positions directly from execute_order
				SELECT DISTINCT
					execute_order.trader_addr,
					execute_order.user_position
				FROM
					stormtrade_ton.execute_order AS execute_order
			) AS combined_positions
	),
	-- Tracking position sizes
	position_sizes AS (
		SELECT DISTINCT
			user_position,
			direction,
			position_size,
			tx_lt AS created_lt
		FROM
			(
				SELECT
					user_position,
					direction,
					position_size,
					tx_lt
				FROM
					storm_update_position
				UNION
				SELECT
					user_position,
					direction,
					position_size,
					tx_lt
				FROM
					storm_complete_order
			) AS combined_positions
	),
	-- Find previous positions for each current position
	previous_position AS (
		SELECT
			user_position,
			direction,
			created_lt,
			COALESCE(
				LAG (position_size, 1) OVER (
					PARTITION BY
						user_position,
						direction
					ORDER BY
						created_lt ASC
				),
				0
			) AS previous_position_size
		FROM
			position_sizes
	),
	-- Combined event data with PNL calculations
	storm_events AS (
		-- Position opening events
		SELECT
			tx_hash,
			positions.trader_addr,
			0 AS pnl, -- Opening positions don't have PNL
			update_position.position_fee / 1e9 AS fee_ton
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
			JOIN previous_position AS prev ON prev.user_position = update_position.user_position
			AND prev.direction = update_position.direction
			AND prev.created_lt = update_position.tx_lt
		WHERE
			update_position.origin_op = 2774268195
		UNION ALL
		-- Position closing events with PNL calculation
		SELECT
			tx_hash,
			positions.trader_addr,
			-- Calculate PNL based on direction and price changes
			CASE
				WHEN update_position.direction = 0 THEN -- Long position
				ABS(
					prev.previous_position_size - update_position.position_size
				) * (
					1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve - COALESCE(
						LAG (
							1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve
						) OVER (
							PARTITION BY
								update_position.user_position
							ORDER BY
								update_position.tx_lt
						),
						1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve
					)
				) / 1e18
				WHEN update_position.direction = 1 THEN -- Short position
				ABS(
					prev.previous_position_size - update_position.position_size
				) * (
					COALESCE(
						LAG (
							1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve
						) OVER (
							PARTITION BY
								update_position.user_position
							ORDER BY
								update_position.tx_lt
						),
						1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve
					) - 
						1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve
				) / 1e18
				ELSE 0
			END AS pnl,
			update_position.position_fee / 1e9 AS fee_ton
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
			JOIN previous_position AS prev ON prev.user_position = update_position.user_position
			AND prev.direction = update_position.direction
			AND prev.created_lt = update_position.tx_lt
		WHERE
			update_position.origin_op IN (1556101853, 3427973859)
		UNION ALL
		-- Take Profit and Stop Loss events with PNL calculation
		SELECT
			tx_hash,
			positions.trader_addr,
			-- Calculate PNL based on direction and price changes
			CASE
				WHEN complete_order.direction = 0 THEN -- Long position
				ABS(
					prev.previous_position_size - complete_order.position_size
				) * (
					1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve - COALESCE(
						LAG (
							1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve
						) OVER (
							PARTITION BY
								complete_order.user_position
							ORDER BY
								complete_order.tx_lt
						),
						1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve
					)
				) / 1e18
				WHEN complete_order.direction = 1 THEN -- Short position
				ABS(
					prev.previous_position_size - complete_order.position_size
				) * (
					COALESCE(
						LAG (
							1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve
						) OVER (
							PARTITION BY
								complete_order.user_position
							ORDER BY
								complete_order.tx_lt
						),
						1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve
					) - 
						1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve
				) / 1e18
				ELSE 0
			END AS pnl,
			complete_order.position_fee / 1e9 AS fee_ton
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
			JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
			AND prev.direction = complete_order.direction
			AND prev.created_lt = complete_order.tx_lt
		WHERE
			complete_order.origin_op = 1556101853
		UNION ALL
		-- Limit order opening events
		SELECT
			tx_hash,
			positions.trader_addr,
			0 AS pnl, -- Opening positions don't have PNL yet
			complete_order.position_fee / 1e9 AS fee_ton
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
			JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
			AND prev.direction = complete_order.direction
			AND prev.created_lt = complete_order.tx_lt
		WHERE
			complete_order.origin_op = 2774268195
	),
	-- Get most recent TON price as fallback
	ton_price_recent AS (
		SELECT
			price_usd * 1e9 AS recent_ton_price -- Multiply by 1e9 to convert from nanoTON to TON
		FROM
			ton.prices_daily
		WHERE
			token_address = '0:0000000000000000000000000000000000000000000000000000000000000000'
			AND price_usd > 0
		ORDER BY
			timestamp DESC
		LIMIT
			1
	),
	-- Aggregate PNL by trader address
	trader_pnl AS (
		SELECT
			trader_addr,
			SUM(pnl) AS total_pnl_ton,
			SUM(fee_ton) AS total_fees_ton,
			COUNT(DISTINCT tx_hash) AS total_transactions,
			(SUM(pnl) - SUM(fee_ton)) AS net_pnl_ton
		FROM
			storm_events
		GROUP BY
			trader_addr
	),
	-- For Bar Chart: Separate profitable and losing traders
	trader_performance AS (
		SELECT
			CASE
				WHEN net_pnl_ton > 0 THEN 'Profitable Traders'
				ELSE 'Losing Traders'
			END AS trader_category,
			COUNT(*) AS trader_count,
			SUM(net_pnl_ton) AS actual_pnl_value,
			AVG(net_pnl_ton) AS avg_pnl_per_trader,
			MIN(net_pnl_ton) AS min_pnl,
			MAX(net_pnl_ton) AS max_pnl
		FROM
			trader_pnl
		GROUP BY
			CASE
				WHEN net_pnl_ton > 0 THEN 'Profitable Traders'
				ELSE 'Losing Traders'
			END
	)
	-- Final output for Bar Chart display
SELECT
	trader_category,
	trader_count,
	actual_pnl_value AS pnl_ton,
	actual_pnl_value * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS pnl_usd,
	avg_pnl_per_trader AS avg_pnl_ton,
	avg_pnl_per_trader * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS avg_pnl_usd,
	min_pnl AS min_pnl_ton,
	min_pnl * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS min_pnl_usd,
	max_pnl AS max_pnl_ton,
	max_pnl * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS max_pnl_usd
FROM
	trader_performance
ORDER BY
	trader_category DESC;