-- [STORM] Daily Traders by Type
-- One-time (1d) -- Only one day of activity; New (last 7d) -- First activity within the last 7 days; Active (10d +) -- Active for more than 30 days and were active for more than 10 days; Others -- Other traders
WITH
	-- Base tables
	storm_update_position AS (
		SELECT
			block_date,
			tx_hash,
			tx_lt,
			tx_now,
			user_position,
			direction,
			origin_op
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
			origin_op
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
			update_position.user_position
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
		WHERE
			update_position.origin_op = 2774268195
	),
	-- Position closing events
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
			update_position.user_position
		FROM
			storm_update_position AS update_position
			JOIN positions ON positions.user_position = update_position.user_position
		WHERE
			update_position.origin_op IN (1556101853, 3427973859)
	),
	-- Take Profit and Stop Loss events
	take_profit_stop_loss AS (
		SELECT
			tx_hash,
			complete_order.order_type AS event_type,
			positions.trader_addr,
			complete_order.tx_lt AS created_lt,
			complete_order.tx_now,
			complete_order.direction,
			complete_order.user_position
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
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
			complete_order.user_position
		FROM
			storm_complete_order AS complete_order
			JOIN positions ON positions.user_position = complete_order.user_position
		WHERE
			complete_order.origin_op = 2774268195
	),
	-- Combined event data
	storm_events AS (
		SELECT
			*
		FROM
			position_open
		UNION ALL
		SELECT
			*
		FROM
			position_close
		UNION ALL
		SELECT
			*
		FROM
			take_profit_stop_loss
		UNION ALL
		SELECT
			*
		FROM
			open_limit
	),
	-- Trader activity analysis
	trader_activity AS (
		SELECT
			trader_addr,
			MIN(DATE_TRUNC ('day', FROM_UNIXTIME (tx_now))) AS first_active_day,
			MAX(DATE_TRUNC ('day', FROM_UNIXTIME (tx_now))) AS last_active_day,
			COUNT(
				DISTINCT DATE_TRUNC ('day', FROM_UNIXTIME (tx_now))
			) AS active_days,
			DATE_TRUNC ('day', NOW ()) AS current_day
		FROM
			storm_events
		GROUP BY
			trader_addr
	),
	-- Trader classification by activity
	trader_classification AS (
		SELECT
			trader_addr,
			active_days,
			first_active_day,
			last_active_day,
			CASE
				WHEN active_days = 1 THEN 'One-time (1d)' -- Only one day of activity
				WHEN DATE_DIFF ('day', first_active_day, current_day) <= 7 THEN 'New (last 7d)' -- First activity within the last 7 days
				WHEN DATE_DIFF ('day', first_active_day, current_day) > 30
				AND active_days > 10 THEN 'Active (10d+)' -- Active for more than 30 days and were active for more than 10 days
				ELSE 'Others' -- Other traders
			END AS trader_type
		FROM
			trader_activity
	)
	-- Final query - daily trader count by category
SELECT
	DATE_TRUNC ('day', FROM_UNIXTIME (tx_now)) AS day,
	trader_type,
	COUNT(DISTINCT trader_addr) AS daily_traders
FROM
	(
		SELECT
			e.*,
			c.trader_type
		FROM
			storm_events e
			JOIN trader_classification c ON e.trader_addr = c.trader_addr
	) AS classified_events
WHERE
	DATE_TRUNC ('day', FROM_UNIXTIME (tx_now)) >= DATE_TRUNC ('day', NOW () - INTERVAL '{{days}}' DAY)
GROUP BY
	DATE_TRUNC ('day', FROM_UNIXTIME (tx_now)),
	trader_type
ORDER BY
	DATE_TRUNC ('day', FROM_UNIXTIME (tx_now)) DESC,
	trader_type;