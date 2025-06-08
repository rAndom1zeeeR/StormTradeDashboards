-- [STORM] Daily Traders Fees Counters
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
	-- Tracking position sizes - simplified with pre-filtered data
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
	-- Create price calculation helper function to avoid repetition
	price_calc AS (
		SELECT
			user_position,
			tx_lt,
			tx_now,
			position_fee,
			CAST(FROM_UNIXTIME (tx_now) AS DATE) AS event_date,
			trader_addr,
			event_type
		FROM
			(
				-- Position opening events
				SELECT
					positions.trader_addr,
					update_position.tx_lt,
					update_position.tx_now,
					update_position.position_fee,
					update_position.user_position,
					'open' AS event_type
				FROM
					storm_update_position AS update_position
					JOIN positions ON positions.user_position = update_position.user_position
					JOIN previous_position AS prev ON prev.user_position = update_position.user_position
					AND prev.direction = update_position.direction
					AND prev.created_lt = update_position.tx_lt
				WHERE
					update_position.origin_op = 2774268195
				UNION ALL
				-- Position closing events
				SELECT
					positions.trader_addr,
					update_position.tx_lt,
					update_position.tx_now,
					update_position.position_fee,
					update_position.user_position,
					CASE
						WHEN origin_op = 1556101853 THEN 'close'
						ELSE 'liquidation'
					END AS event_type
				FROM
					storm_update_position AS update_position
					JOIN positions ON positions.user_position = update_position.user_position
					JOIN previous_position AS prev ON prev.user_position = update_position.user_position
					AND prev.direction = update_position.direction
					AND prev.created_lt = update_position.tx_lt
				WHERE
					update_position.origin_op IN (1556101853, 3427973859)
				UNION ALL
				-- Take Profit and Stop Loss events
				SELECT
					positions.trader_addr,
					complete_order.tx_lt,
					complete_order.tx_now,
					complete_order.position_fee,
					complete_order.user_position,
					complete_order.order_type AS event_type
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
					positions.trader_addr,
					complete_order.tx_lt,
					complete_order.tx_now,
					complete_order.position_fee,
					complete_order.user_position,
					'open' AS event_type
				FROM
					storm_complete_order AS complete_order
					JOIN positions ON positions.user_position = complete_order.user_position
					JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
					AND prev.direction = complete_order.direction
					AND prev.created_lt = complete_order.tx_lt
				WHERE
					complete_order.origin_op = 2774268195
			) AS all_events
	),
	-- Calculate fees with USD value directly using ton.prices_daily for accuracy
	-- Using most recent price if no data available in period
	ton_price_period AS (
		SELECT
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
	),
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
	ton_price AS (
		SELECT
			COALESCE(
				(
					SELECT
						avg_ton_price
					FROM
						ton_price_period
				),
				(
					SELECT
						recent_ton_price
					FROM
						ton_price_recent
				)
			) AS avg_ton_price
	)
	-- Final results
SELECT
	COALESCE(SUM(position_fee) / 1e9, 0) AS total_fees_ton,
	COALESCE(
		SUM(position_fee) / 1e9 * (
			SELECT
				avg_ton_price
			FROM
				ton_price
		),
		0
	) AS total_fees_usd,
	COUNT(DISTINCT trader_addr) AS unique_traders,
	COUNT(*) AS total_transactions
FROM
	price_calc
WHERE
	event_date >= (
		SELECT
			start_date
		FROM
			date_range
	)