-- [STORM] Daily Traders Counter
WITH
	-- Base tables with preliminary date filtering
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
			block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
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
			block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
	),
	storm_execute_order AS (
		SELECT
			tx_hash,
			user_position,
			trader_addr
		FROM
			stormtrade_ton.execute_order
		WHERE
			block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
	),
	storm_trade_notification AS (
		SELECT
			tx_hash,
			trader_addr
		FROM
			stormtrade_ton.trade_notification
		WHERE
			block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
	),
	-- Combined unique traders from execute_order and trade_notification
	all_traders AS (
		SELECT DISTINCT
			trader_addr
		FROM
			stormtrade_ton.execute_order
		WHERE
			block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
		UNION
		SELECT DISTINCT
			trader_addr
		FROM
			stormtrade_ton.trade_notification
		WHERE
			block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
	)
	-- Final count
SELECT
	COUNT(*) AS total_traders
FROM
	all_traders