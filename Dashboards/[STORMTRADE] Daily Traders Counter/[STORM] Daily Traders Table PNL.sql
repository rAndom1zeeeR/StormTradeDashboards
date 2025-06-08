-- [STORM] Daily Traders Table PNL
WITH
	-- Pre-filter by date range to reduce data processing
	date_range AS (
		SELECT
			CURRENT_DATE - INTERVAL '{{days}}' day AS start_date
	),
	-- Base tables with early date filtering
	storm_update_position AS (
		SELECT
			block_date,
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
	storm_execute_order AS (
		SELECT
			tx_hash,
			user_position,
			trader_addr,
			tx_now
		FROM
			stormtrade_ton.execute_order
		WHERE
			CAST(FROM_UNIXTIME (tx_now) AS DATE) >= (
				SELECT
					start_date
				FROM
					date_range
			)
	),
	storm_trade_notification AS (
		SELECT
			tx_hash,
			trader_addr,
			tx_now
		FROM
			stormtrade_ton.trade_notification
		WHERE
			CAST(FROM_UNIXTIME (tx_now) AS DATE) >= (
				SELECT
					start_date
				FROM
					date_range
			)
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
	-- Relationship between positions and traders (optimized without ton.messages)
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
	-- Position opening events
	position_open AS (
		SELECT
			tx_hash,
			'open' AS event_type,
			positions.trader_addr,
			update_position.tx_lt AS created_lt,
			update_position.tx_now,
			update_position.direction,
			update_position.position_size,
			1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve AS oracle_price,
			COALESCE(
				ABS(
					prev.previous_position_size - update_position.position_size
				) * 1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve / 1e18,
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
		WHERE
			update_position.origin_op = 2774268195
	),
	-- Position closing events with PNL calculation
	position_close AS (
		SELECT
			tx_hash,
			CASE
				WHEN origin_op = 1556101853 THEN 'close'
				ELSE 'liquidation'
			END AS event_type,
			positions.trader_addr,
			update_position.tx_lt AS created_lt,
			update_position.tx_now,
			update_position.direction,
			update_position.position_size,
			1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve AS oracle_price,
			ABS(
				prev.previous_position_size - update_position.position_size
			) * 1.0 * update_position.quote_asset_weight * update_position.quote_asset_reserve / update_position.base_asset_reserve / 1e18 AS volume,
			update_position.position_fee,
			update_position.user_position,
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
			DATE_TRUNC ('day', FROM_UNIXTIME (update_position.tx_now)) AS tx_day
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
			JOIN previous_position AS prev ON prev.user_position = update_position.user_position
			AND prev.direction = update_position.direction
			AND prev.created_lt = update_position.tx_lt
		WHERE
			update_position.origin_op IN (1556101853, 3427973859)
	),
	-- Take Profit and Stop Loss events with PNL calculation
	take_profit_stop_loss AS (
		SELECT
			tx_hash,
			complete_order.order_type AS event_type,
			positions.trader_addr,
			complete_order.tx_lt AS created_lt,
			complete_order.tx_now,
			complete_order.direction,
			complete_order.position_size,
			1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve AS oracle_price,
			ABS(
				prev.previous_position_size - complete_order.position_size
			) * 1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve / 1e18 AS volume,
			complete_order.position_fee,
			complete_order.user_position,
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
			DATE_TRUNC ('day', FROM_UNIXTIME (complete_order.tx_now)) AS tx_day
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
			JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
			AND prev.direction = complete_order.direction
			AND prev.created_lt = complete_order.tx_lt
		WHERE
			complete_order.origin_op = 1556101853
	),
	-- Limit order opening events
	open_limit AS (
		SELECT
			tx_hash,
			'open' AS event_type,
			positions.trader_addr,
			complete_order.tx_lt AS created_lt,
			complete_order.tx_now,
			complete_order.direction,
			complete_order.position_size,
			1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve AS oracle_price,
			COALESCE(
				ABS(
					prev.previous_position_size - complete_order.position_size
				) * 1.0 * complete_order.quote_asset_weight * complete_order.quote_asset_reserve / complete_order.base_asset_reserve / 1e18,
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
	-- Get TON prices for USD conversion
	ton_price_daily AS (
		SELECT
			CAST(timestamp AS DATE) AS price_date,
			AVG(price_usd) * 1e9 AS avg_ton_price -- Multiply by 1e9 to convert from nanoTON to TON
		FROM
			ton.prices_daily
		WHERE
			token_address = '0:0000000000000000000000000000000000000000000000000000000000000000'
			AND price_usd > 0
			AND CAST(timestamp AS DATE) >= (
				SELECT
					start_date
				FROM
					date_range
			)
		GROUP BY
			CAST(timestamp AS DATE)
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
	-- Prepare daily prices for join
	daily_prices AS (
		SELECT DISTINCT
			events.tx_day,
			COALESCE(
				prices.avg_ton_price,
				(
					SELECT
						recent_ton_price
					FROM
						ton_price_recent
				)
			) AS ton_price
		FROM
			storm_events events
			LEFT JOIN ton_price_daily prices ON events.tx_day = prices.price_date
	),
	-- Get all storm transactions by trader (using only tables that exist)
	all_storm_transactions AS (
		-- Trade Notification transactions
		SELECT
			trader_addr,
			tx_hash,
			CAST(FROM_UNIXTIME (tx_now) AS DATE) AS tx_day
		FROM
			storm_trade_notification
		UNION
		-- Execute Order transactions
		SELECT
			trader_addr,
			tx_hash,
			CAST(FROM_UNIXTIME (tx_now) AS DATE) AS tx_day
		FROM
			storm_execute_order
	),
	-- Calculate daily transaction counts more accurately
	daily_transactions AS (
		SELECT
			tx_day,
			trader_addr,
			COUNT(DISTINCT tx_hash) AS daily_transaction_count
		FROM
			all_storm_transactions
		GROUP BY
			tx_day,
			trader_addr
	)
	-- Final query - daily PNL by trader
SELECT
	events.tx_day AS day,
	CONCAT (
		'<a href="https://tonviewer.com/',
		events.trader_addr,
		'" target="_blank">',
		events.trader_addr,
		'</a>'
	) AS trader_address,
	SUM(events.pnl) AS daily_pnl_ton,
	SUM(events.pnl) * prices.ton_price AS daily_pnl_usd,
	SUM(events.position_fee) / 1e9 AS daily_fees_ton,
	SUM(events.position_fee) / 1e9 * prices.ton_price AS daily_fees_usd,
	tx_counts.daily_transaction_count AS daily_transactions_count
FROM
	storm_events events
	JOIN daily_prices prices ON events.tx_day = prices.tx_day
	JOIN daily_transactions tx_counts ON events.tx_day = tx_counts.tx_day
	AND events.trader_addr = tx_counts.trader_addr
WHERE
	events.tx_day >= DATE_TRUNC ('day', NOW () - INTERVAL '{{days}}' DAY)
GROUP BY
	events.tx_day,
	events.trader_addr,
	prices.ton_price,
	tx_counts.daily_transaction_count
ORDER BY
	events.tx_day DESC,
	daily_pnl_ton DESC;