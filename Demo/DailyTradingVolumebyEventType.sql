WITH
	-- Base tables
	storm_update_position AS (
		SELECT
			*
		FROM
			stormtrade_ton.update_position
	),
	storm_complete_order AS (
		SELECT
			*
		FROM
			stormtrade_ton.complete_order
	),
	storm_execute_order AS (
		SELECT
			*
		FROM
			stormtrade_ton.execute_order
	),
	storm_trade_notification AS (
		SELECT
			*
		FROM
			stormtrade_ton.trade_notification
	),
	-- Position sizes tracking
	position_sizes_raw AS (
		SELECT
			user_position,
			sup.direction,
			position_size,
			tx_lt AS created_lt
		FROM
			storm_update_position sup
		UNION
		SELECT
			user_position AS position_addr,
			sco.direction,
			position_size,
			tx_lt AS created_lt
		FROM
			storm_complete_order sco
	),
	position_sizes AS (
		SELECT DISTINCT
			*
		FROM
			position_sizes_raw
	),
	prev_position AS (
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
			) AS prev_position
		FROM
			position_sizes
	),
	-- Position to trader mapping
	positions_raw AS (
		SELECT DISTINCT
			stn.trader_addr,
			sup.user_position
		FROM
			storm_update_position sup
			JOIN ton.messages notif_message_out ON notif_message_out.block_date = sup.block_date
			AND notif_message_out.tx_hash = sup.tx_hash
			AND notif_message_out.direction = 'out' -- vAMM -> vault
			JOIN ton.messages notif_message_in ON notif_message_out.block_date = notif_message_in.block_date
			AND notif_message_out.msg_hash = notif_message_in.msg_hash
			AND notif_message_out.direction = 'in' -- incoming messages for vault
			JOIN storm_trade_notification stn ON stn.tx_hash = notif_message_out.tx_hash
		WHERE
			origin_op != 3427973859
		UNION
		SELECT DISTINCT
			seo.trader_addr,
			user_position
		FROM
			storm_execute_order seo
	),
	positions AS (
		SELECT DISTINCT
			trader_addr,
			user_position
		FROM
			positions_raw
		GROUP BY
			1,
			2
	),
	-- Position events
	position_open AS (
		SELECT
			tx_hash,
			'open' AS event_type,
			positions.trader_addr,
			tx_lt AS created_lt,
			tx_now,
			sup.direction,
			position_size,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price,
			COALESCE(
				ABS(prev_position - position_size) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve / 1e18,
				position_open_notional / 1e9
			) AS volume,
			position_fee,
			sup.user_position
		FROM
			storm_update_position sup
			JOIN positions ON positions.user_position = sup.user_position
			JOIN prev_position pp ON pp.user_position = sup.user_position
			AND pp.direction = sup.direction
			AND pp.created_lt = sup.tx_lt
		WHERE
			origin_op = 2774268195
	),
	position_close AS (
		SELECT
			tx_hash,
			CASE
				WHEN origin_op = 1556101853 THEN 'close'
				ELSE 'liquidation'
			END AS event_type,
			positions.trader_addr,
			tx_lt AS created_lt,
			tx_now,
			sup.direction,
			position_size,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price,
			ABS(prev_position - position_size) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve / 1e18 AS volume,
			position_fee,
			sup.user_position
		FROM
			storm_update_position sup
			JOIN positions ON positions.user_position = sup.user_position
			JOIN prev_position pp ON pp.user_position = sup.user_position
			AND pp.direction = sup.direction
			AND pp.created_lt = sup.tx_lt
		WHERE
			origin_op IN (1556101853, 3427973859)
	),
	tp_sl AS (
		SELECT
			tx_hash,
			order_type AS event_type,
			positions.trader_addr,
			tx_lt AS created_lt,
			tx_now,
			sco.direction,
			position_size,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price,
			ABS(prev_position - position_size) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve / 1e18 AS volume,
			position_fee,
			sco.user_position
		FROM
			storm_complete_order sco
			JOIN positions ON positions.user_position = sco.user_position
			JOIN prev_position pp ON pp.user_position = sco.user_position
			AND pp.direction = sco.direction
			AND pp.created_lt = sco.tx_lt
		WHERE
			origin_op = 1556101853
	),
	open_limit AS (
		SELECT
			tx_hash,
			'open' AS event_type,
			positions.trader_addr,
			tx_lt AS created_lt,
			tx_now,
			sco.direction,
			position_size,
			1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve AS oracle_price,
			COALESCE(
				ABS(prev_position - position_size) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve / 1e18,
				position_open_notional / 1e9
			) AS volume,
			position_fee,
			sco.user_position
		FROM
			storm_complete_order sco
			JOIN positions ON positions.user_position = sco.user_position
			JOIN prev_position pp ON pp.user_position = sco.user_position
			AND pp.direction = sco.direction
			AND pp.created_lt = sco.tx_lt
		WHERE
			origin_op = 2774268195
	),
	-- Combined events data
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
			tp_sl
		UNION ALL
		SELECT
			*
		FROM
			open_limit
	)
	-- Final query - daily volumes by event type
SELECT
	DATE_TRUNC ('day', FROM_UNIXTIME (tx_now)) AS day,
	event_type,
	SUM(volume) AS total_volume
FROM
	storm_events
WHERE
	DATE_TRUNC ('day', FROM_UNIXTIME (tx_now)) >= DATE_TRUNC ('day', NOW () - INTERVAL '{{days}}' DAY)
GROUP BY
	1,
	2
ORDER BY
	day DESC,
	event_type;