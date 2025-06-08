-- [STORM] Total Traders Counter
WITH
	-- Unique traders from execute_order without date filtering
	execute_order_traders AS (
		SELECT DISTINCT
			trader_addr
		FROM
			stormtrade_ton.execute_order
	),
	-- Unique traders from trade_notification without date filtering
	trade_notification_traders AS (
		SELECT DISTINCT
			trader_addr
		FROM
			stormtrade_ton.trade_notification
	),
	-- Combine all traders
	all_traders AS (
		SELECT
			trader_addr
		FROM
			execute_order_traders
		UNION
		SELECT
			trader_addr
		FROM
			trade_notification_traders
	)
	-- Final count
SELECT
	COUNT(*) AS total_traders
FROM
	all_traders