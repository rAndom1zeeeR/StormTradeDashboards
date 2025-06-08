-- [STORM] Total Traders Fees Counters
WITH
	-- Base tables without date filtering, optimize by selecting only needed columns
	storm_update_position AS (
		SELECT
			tx_hash,
			tx_lt,
			tx_now,
			user_position,
			direction,
			origin_op,
			position_size,
			position_fee
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
			position_fee
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
	-- Tracking position sizes with optimized column selection
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
	-- Optimized price calculation by reducing columns and simplifying expressions
	price_calc AS (
		SELECT
			position_fee,
			trader_addr
		FROM
			(
				-- Position opening events
				SELECT
					positions.trader_addr,
					update_position.position_fee
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
					update_position.position_fee
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
					complete_order.position_fee
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
					complete_order.position_fee
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
	-- Using most recent price and optimizing the lookup
	ton_price AS (
		SELECT
			AVG(price_usd) * 1e9 AS avg_ton_price -- Multiply by 1e9 to convert from nanoTON to TON
		FROM
			ton.prices_daily
		WHERE
			token_address = '0:0000000000000000000000000000000000000000000000000000000000000000'
			AND price_usd > 0
	)
	-- Final results with optimized calculation
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