-- [STORM] Total Trade Fees
-- This query retrieves all trading fees data from Storm trading platform
WITH
	-- Relationship between positions and traders - optimized query
	positions AS (
		SELECT DISTINCT
			tn.trader_addr,
			up.user_position
		FROM
			stormtrade_ton.update_position up
			JOIN stormtrade_ton.trade_notification tn ON tn.tx_hash = up.tx_hash
		WHERE
			up.origin_op != 3427973859
		UNION
		SELECT DISTINCT
			eo.trader_addr,
			eo.user_position
		FROM
			stormtrade_ton.execute_order eo
			JOIN stormtrade_ton.update_position up ON up.user_position = eo.user_position
	),
	-- Simplified tracking of position events with integrated filtering
	all_trading_events AS (
		-- Open positions from update_position
		SELECT
			p.trader_addr,
			up.tx_now,
			up.position_fee,
			CAST(FROM_UNIXTIME (up.tx_now) AS DATE) AS event_date
		FROM
			stormtrade_ton.update_position up
			JOIN positions p ON p.user_position = up.user_position
		WHERE
			up.origin_op = 2774268195
		UNION ALL
		-- Close and liquidation from update_position
		SELECT
			p.trader_addr,
			up.tx_now,
			up.position_fee,
			CAST(FROM_UNIXTIME (up.tx_now) AS DATE) AS event_date
		FROM
			stormtrade_ton.update_position up
			JOIN positions p ON p.user_position = up.user_position
		WHERE
			up.origin_op IN (1556101853, 3427973859)
		UNION ALL
		-- Close from complete_order
		SELECT
			p.trader_addr,
			co.tx_now,
			co.position_fee,
			CAST(FROM_UNIXTIME (co.tx_now) AS DATE) AS event_date
		FROM
			stormtrade_ton.complete_order co
			JOIN positions p ON p.user_position = co.user_position
		WHERE
			co.origin_op = 1556101853
		UNION ALL
		-- Open from complete_order
		SELECT
			p.trader_addr,
			co.tx_now,
			co.position_fee,
			CAST(FROM_UNIXTIME (co.tx_now) AS DATE) AS event_date
		FROM
			stormtrade_ton.complete_order co
			JOIN positions p ON p.user_position = co.user_position
		WHERE
			co.origin_op = 2774268195
	),
	-- Get TON price data for fee conversion to USD - simplified with materialized CTE
	ton_prices AS (
		SELECT
			CAST(timestamp AS DATE) AS price_date,
			AVG(price_usd) * 1e9 AS ton_price_usd -- Convert from nanoTON to TON
		FROM
			ton.prices_daily
		WHERE
			token_address = '0:0000000000000000000000000000000000000000000000000000000000000000'
			AND price_usd > 0
		GROUP BY
			CAST(timestamp AS DATE)
	),
	-- Get the most recent price for days without price data
	recent_price AS (
		SELECT
			price_usd * 1e9 AS recent_ton_price
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
						ton_prices
					WHERE
						price_date = event_date
				),
				(
					SELECT
						recent_ton_price
					FROM
						recent_price
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
ORDER BY
	event_date DESC;