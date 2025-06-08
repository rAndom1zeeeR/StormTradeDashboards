-- [STORM] Total Traders Table PNL by Address
WITH
	-- Base tables without date filtering - removed unused fields
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
	-- Tracking position sizes - optimized query
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
	-- Relationship between positions and traders (optimized)
	positions AS (
		SELECT DISTINCT
			trader_addr,
			user_position
		FROM
			(
				-- Get positions from update_position and trade_notification directly
				SELECT DISTINCT
					notification.trader_addr,
					update_position.user_position
				FROM
					storm_update_position AS update_position
					JOIN storm_trade_notification AS notification ON notification.tx_hash = update_position.tx_hash
				WHERE
					update_position.origin_op != 3427973859
				UNION
				-- Get positions directly from execute_order
				SELECT DISTINCT
					execute_order.trader_addr,
					execute_order.user_position
				FROM
					storm_execute_order AS execute_order
			) AS combined_positions
	),
	-- Calculate oracle price once to avoid repetition
	oracle_prices AS (
		SELECT
			'update_position' AS source,
			storm_update_position.tx_hash,
			storm_update_position.tx_lt,
			storm_update_position.user_position,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price
		FROM
			storm_update_position
		UNION ALL
		SELECT
			'complete_order' AS source,
			storm_complete_order.tx_hash,
			storm_complete_order.tx_lt,
			storm_complete_order.user_position,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price
		FROM
			storm_complete_order
	),
	-- Position opening events
	position_open AS (
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
	),
	-- Position closing events with PNL calculation
	position_close AS (
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
	),
	-- Take Profit and Stop Loss events with PNL calculation
	take_profit_stop_loss AS (
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
	),
	-- Limit order opening events
	open_limit AS (
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
			0 AS pnl, -- Opening positions don't have PNL yet
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
	-- Combined event data
	storm_events AS (
		SELECT
			tx_hash,
			event_type,
			trader_addr,
			created_lt,
			tx_now,
			direction,
			position_size,
			oracle_price,
			volume,
			position_fee,
			user_position,
			0 AS pnl, -- Opening positions don't have PNL
			tx_day
		FROM
			position_open
		UNION ALL
		SELECT
			tx_hash,
			event_type,
			trader_addr,
			created_lt,
			tx_now,
			direction,
			position_size,
			oracle_price,
			volume,
			position_fee,
			user_position,
			pnl,
			tx_day
		FROM
			position_close
		UNION ALL
		SELECT
			tx_hash,
			event_type,
			trader_addr,
			created_lt,
			tx_now,
			direction,
			position_size,
			oracle_price,
			volume,
			position_fee,
			user_position,
			pnl,
			tx_day
		FROM
			take_profit_stop_loss
		UNION ALL
		SELECT
			tx_hash,
			event_type,
			trader_addr,
			created_lt,
			tx_now,
			direction,
			position_size,
			oracle_price,
			volume,
			position_fee,
			user_position,
			pnl,
			tx_day
		FROM
			open_limit
	),
	-- Get most recent TON price
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
	-- Get unique transaction counts per trader - optimized by using storm_events
	transaction_counts AS (
		SELECT
			trader_addr,
			COUNT(DISTINCT tx_hash) AS tx_count
		FROM
			storm_events
		GROUP BY
			trader_addr
	),
	-- Calculate total trading volume per trader
	trader_volumes AS (
		SELECT
			trader_addr,
			SUM(volume) AS total_volume
		FROM
			storm_events
		GROUP BY
			trader_addr
	),
	-- Get first and last trade dates per trader
	trader_dates AS (
		SELECT
			trader_addr,
			MIN(FROM_UNIXTIME (tx_now)) AS first_trade,
			MAX(FROM_UNIXTIME (tx_now)) AS last_trade
		FROM
			storm_events
		GROUP BY
			trader_addr
	)
	-- Final query - total PNL by trader
SELECT
	CONCAT (
		'<a href="https://tonviewer.com/',
		events.trader_addr,
		'" target="_blank">',
		events.trader_addr,
		'</a>'
	) AS trader_address,
	SUM(events.pnl) AS total_pnl_ton,
	SUM(events.pnl) * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS total_pnl_usd,
	SUM(events.position_fee) / 1e9 AS total_fees_ton,
	SUM(events.position_fee) / 1e9 * (
		SELECT
			recent_ton_price
		FROM
			ton_price_recent
	) AS total_fees_usd,
	COALESCE(vol.total_volume, 0) AS total_volume,
	COALESCE(tx.tx_count, 0) AS transactions_count,
	CAST(td.first_trade AS DATE) AS first_trade_date,
	CAST(td.last_trade AS DATE) AS last_trade_date
FROM
	storm_events events
	LEFT JOIN transaction_counts tx ON events.trader_addr = tx.trader_addr
	LEFT JOIN trader_volumes vol ON events.trader_addr = vol.trader_addr
	LEFT JOIN trader_dates td ON events.trader_addr = td.trader_addr
GROUP BY
	events.trader_addr,
	tx.tx_count,
	vol.total_volume,
	td.first_trade,
	td.last_trade
ORDER BY
	total_pnl_ton DESC;