-- [STORM] Total Profitable Traders Table
WITH
	-- Base tables with only necessary fields
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
	),
	storm_execute_order AS (
		SELECT
			tx_hash,
			user_position,
			trader_addr
		FROM
			stormtrade_ton.execute_order
	),
	storm_trade_notification AS (
		SELECT
			tx_hash,
			trader_addr
		FROM
			stormtrade_ton.trade_notification
	),
	-- Calculate oracle price once to avoid repetition
	oracle_prices AS (
		SELECT
			'update_position' AS source,
			tx_hash,
			tx_lt,
			user_position,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price
		FROM
			storm_update_position
		UNION ALL
		SELECT
			'complete_order' AS source,
			tx_hash,
			tx_lt,
			user_position,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price
		FROM
			storm_complete_order
	),
	-- Relationship between positions and traders (optimized)
	positions AS (
		SELECT DISTINCT
			trader_addr,
			user_position
		FROM
			(
				SELECT DISTINCT
					notification.trader_addr,
					update_position.user_position
				FROM
					storm_update_position AS update_position
					JOIN storm_trade_notification AS notification ON notification.tx_hash = update_position.tx_hash
				WHERE
					update_position.origin_op != 3427973859
				UNION
				SELECT DISTINCT
					execute_order.trader_addr,
					execute_order.user_position
				FROM
					storm_execute_order AS execute_order
			) AS combined_positions
	),
	-- Tracking position sizes - optimized with single combined query
	position_sizes AS (
		SELECT
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
				UNION ALL
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
	-- Combined events data with position opening
	position_events AS (
		-- Position opening events
		SELECT
			update_position.tx_hash,
			'open' AS event_type,
			positions.trader_addr,
			update_position.tx_lt AS created_lt,
			update_position.tx_now,
			update_position.direction,
			update_position.position_size,
			op.oracle_price,
			COALESCE(
				ABS(
					prev.previous_position_size - update_position.position_size
				) * op.oracle_price / 1e18,
				update_position.position_open_notional / 1e9
			) AS volume,
			update_position.position_fee,
			update_position.user_position,
			0 AS pnl,
			DATE_TRUNC ('day', FROM_UNIXTIME (update_position.tx_now)) AS tx_day
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
			JOIN previous_position AS prev ON prev.user_position = update_position.user_position
			AND prev.direction = update_position.direction
			AND prev.created_lt = update_position.tx_lt
			JOIN oracle_prices op ON op.source = 'update_position'
			AND op.tx_hash = update_position.tx_hash
			AND op.tx_lt = update_position.tx_lt
		WHERE
			update_position.origin_op = 2774268195
		UNION ALL
		-- Position closing and liquidation events
		SELECT
			update_position.tx_hash,
			CASE
				WHEN origin_op = 1556101853 THEN 'close'
				ELSE 'liquidation'
			END AS event_type,
			positions.trader_addr,
			update_position.tx_lt AS created_lt,
			update_position.tx_now,
			update_position.direction,
			update_position.position_size,
			op.oracle_price,
			ABS(
				prev.previous_position_size - update_position.position_size
			) * op.oracle_price / 1e18 AS volume,
			update_position.position_fee,
			update_position.user_position,
			-- Calculate PNL based on direction and price changes
			CASE
				WHEN update_position.direction = 0 THEN -- Long position
				ABS(
					prev.previous_position_size - update_position.position_size
				) * (
					op.oracle_price - COALESCE(
						LAG (op.oracle_price) OVER (
							PARTITION BY
								update_position.user_position
							ORDER BY
								update_position.tx_lt
						),
						op.oracle_price
					)
				) / 1e18
				WHEN update_position.direction = 1 THEN -- Short position
				ABS(
					prev.previous_position_size - update_position.position_size
				) * (
					COALESCE(
						LAG (op.oracle_price) OVER (
							PARTITION BY
								update_position.user_position
							ORDER BY
								update_position.tx_lt
						),
						op.oracle_price
					) - op.oracle_price
				) / 1e18
				ELSE 0
			END AS pnl,
			DATE_TRUNC ('day', FROM_UNIXTIME (update_position.tx_now)) AS tx_day
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
			JOIN previous_position AS prev ON prev.user_position = update_position.user_position
			AND prev.direction = update_position.direction
			AND prev.created_lt = update_position.tx_lt
			JOIN oracle_prices op ON op.source = 'update_position'
			AND op.tx_hash = update_position.tx_hash
			AND op.tx_lt = update_position.tx_lt
		WHERE
			update_position.origin_op IN (1556101853, 3427973859)
		UNION ALL
		-- Take Profit and Stop Loss events
		SELECT
			complete_order.tx_hash,
			complete_order.order_type AS event_type,
			positions.trader_addr,
			complete_order.tx_lt AS created_lt,
			complete_order.tx_now,
			complete_order.direction,
			complete_order.position_size,
			op.oracle_price,
			ABS(
				prev.previous_position_size - complete_order.position_size
			) * op.oracle_price / 1e18 AS volume,
			complete_order.position_fee,
			complete_order.user_position,
			-- Calculate PNL based on direction and price changes
			CASE
				WHEN complete_order.direction = 0 THEN -- Long position
				ABS(
					prev.previous_position_size - complete_order.position_size
				) * (
					op.oracle_price - COALESCE(
						LAG (op.oracle_price) OVER (
							PARTITION BY
								complete_order.user_position
							ORDER BY
								complete_order.tx_lt
						),
						op.oracle_price
					)
				) / 1e18
				WHEN complete_order.direction = 1 THEN -- Short position
				ABS(
					prev.previous_position_size - complete_order.position_size
				) * (
					COALESCE(
						LAG (op.oracle_price) OVER (
							PARTITION BY
								complete_order.user_position
							ORDER BY
								complete_order.tx_lt
						),
						op.oracle_price
					) - op.oracle_price
				) / 1e18
				ELSE 0
			END AS pnl,
			DATE_TRUNC ('day', FROM_UNIXTIME (complete_order.tx_now)) AS tx_day
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
			JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
			AND prev.direction = complete_order.direction
			AND prev.created_lt = complete_order.tx_lt
			JOIN oracle_prices op ON op.source = 'complete_order'
			AND op.tx_hash = complete_order.tx_hash
			AND op.tx_lt = complete_order.tx_lt
		WHERE
			complete_order.origin_op = 1556101853
		UNION ALL
		-- Limit order opening events
		SELECT
			complete_order.tx_hash,
			'open' AS event_type,
			positions.trader_addr,
			complete_order.tx_lt AS created_lt,
			complete_order.tx_now,
			complete_order.direction,
			complete_order.position_size,
			op.oracle_price,
			COALESCE(
				ABS(
					prev.previous_position_size - complete_order.position_size
				) * op.oracle_price / 1e18,
				complete_order.position_open_notional / 1e9
			) AS volume,
			complete_order.position_fee,
			complete_order.user_position,
			0 AS pnl,
			DATE_TRUNC ('day', FROM_UNIXTIME (complete_order.tx_now)) AS tx_day
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
			JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
			AND prev.direction = complete_order.direction
			AND prev.created_lt = complete_order.tx_lt
			JOIN oracle_prices op ON op.source = 'complete_order'
			AND op.tx_hash = complete_order.tx_hash
			AND op.tx_lt = complete_order.tx_lt
		WHERE
			complete_order.origin_op = 2774268195
	),
	-- Get most recent TON price - use single efficient query
	ton_price_recent AS (
		SELECT
			price_usd * 1e9 AS recent_ton_price -- Convert from nanoTON to TON
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
	-- Precompute trader statistics in one pass
	trader_stats AS (
		SELECT
			trader_addr,
			SUM(pnl) AS total_pnl_ton,
			SUM(position_fee) / 1e9 AS total_fees_ton,
			SUM(volume) AS total_volume,
			COUNT(DISTINCT tx_hash) AS tx_count,
			MIN(FROM_UNIXTIME (tx_now)) AS first_trade,
			MAX(FROM_UNIXTIME (tx_now)) AS last_trade
		FROM
			position_events
		GROUP BY
			trader_addr
	)
	-- Final result with all trader stats
SELECT
	CONCAT (
		'<a href="https://tonviewer.com/',
		stats.trader_addr,
		'" target="_blank">',
		stats.trader_addr,
		'</a>'
	) AS trader_address,
	stats.total_pnl_ton,
	stats.total_pnl_ton * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS total_pnl_usd,
	stats.total_fees_ton,
	stats.total_fees_ton * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS total_fees_usd,
	stats.total_volume,
	stats.tx_count AS transactions_count,
	CAST(stats.first_trade AS DATE) AS first_trade_date,
	CAST(stats.last_trade AS DATE) AS last_trade_date
FROM
	trader_stats stats
ORDER BY
	total_pnl_ton DESC;