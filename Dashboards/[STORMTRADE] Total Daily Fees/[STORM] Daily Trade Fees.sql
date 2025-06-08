-- [STORM] Daily Trade Fees
-- This query retrieves daily trading fees data from Storm trading platform
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
	-- Collect all trading events with fees
	all_trading_events AS (
		SELECT
			positions.trader_addr,
			update_position.tx_lt,
			update_position.tx_now,
			update_position.position_fee,
			update_position.user_position,
			'open' AS event_type,
			CAST(FROM_UNIXTIME (update_position.tx_now) AS DATE) AS event_date
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
			JOIN previous_position AS prev ON prev.user_position = update_position.user_position
			AND prev.direction = update_position.direction
			AND prev.created_lt = update_position.tx_lt
		WHERE
			update_position.origin_op = 2774268195
		UNION ALL
		SELECT
			positions.trader_addr,
			update_position.tx_lt,
			update_position.tx_now,
			update_position.position_fee,
			update_position.user_position,
			CASE
				WHEN origin_op = 1556101853 THEN 'close'
				ELSE 'liquidation'
			END AS event_type,
			CAST(FROM_UNIXTIME (update_position.tx_now) AS DATE) AS event_date
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
			JOIN previous_position AS prev ON prev.user_position = update_position.user_position
			AND prev.direction = update_position.direction
			AND prev.created_lt = update_position.tx_lt
		WHERE
			update_position.origin_op IN (1556101853, 3427973859)
		UNION ALL
		SELECT
			positions.trader_addr,
			complete_order.tx_lt,
			complete_order.tx_now,
			complete_order.position_fee,
			complete_order.user_position,
			complete_order.order_type AS event_type,
			CAST(FROM_UNIXTIME (complete_order.tx_now) AS DATE) AS event_date
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
			JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
			AND prev.direction = complete_order.direction
			AND prev.created_lt = complete_order.tx_lt
		WHERE
			complete_order.origin_op = 1556101853
		UNION ALL
		SELECT
			positions.trader_addr,
			complete_order.tx_lt,
			complete_order.tx_now,
			complete_order.position_fee,
			complete_order.user_position,
			'open' AS event_type,
			CAST(FROM_UNIXTIME (complete_order.tx_now) AS DATE) AS event_date
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
			JOIN previous_position AS prev ON prev.user_position = complete_order.user_position
			AND prev.direction = complete_order.direction
			AND prev.created_lt = complete_order.tx_lt
		WHERE
			complete_order.origin_op = 2774268195
	),
	-- Get TON price data for fee conversion to USD
	ton_price_daily AS (
		SELECT
			CAST(timestamp AS DATE) AS price_date,
			AVG(price_usd) * 1e9 AS ton_price_usd -- Convert from nanoTON to TON
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
	-- Get the most recent price for days without price data
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
	-- Calculate daily fees with USD conversion
	daily_fees AS (
		SELECT
			event_date,
			SUM(position_fee) / 1e9 AS total_fees_ton, -- Convert from nanoTON to TON
			SUM(position_fee) / 1e9 * COALESCE(
				(
					SELECT
						ton_price_usd
					FROM
						ton_price_daily
					WHERE
						price_date = event_date
				),
				(
					SELECT
						recent_ton_price
					FROM
						ton_price_recent
				)
			) AS total_fees_usd
		FROM
			all_trading_events
		GROUP BY
			event_date
	)
	-- Final result
SELECT
	event_date AS day,
	total_fees_ton,
	total_fees_usd
FROM
	daily_fees
WHERE
	event_date >= (
		SELECT
			start_date
		FROM
			date_range
	)
ORDER BY
	event_date DESC;