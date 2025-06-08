WITH
	closed_positions AS (
		-- Get all closed positions from trade notifications
		SELECT
			tn.block_date,
			tn.tx_hash,
			tn.trader_addr,
			tn.vault,
			tn.vault_token,
			tn.amm,
			tn.asset_id,
			tn.withdraw_amount AS realized_pnl, -- Can be negative for losses
			tn.fee_to_stakers, -- Fee paid to stakers
			tn.exchange_amount, -- Can be used to calculate additional fees
			tn.referral_amount -- Referral fees
		FROM
			stormtrade_ton.trade_notification tn
		WHERE
			-- Optional date filter that can be parameterized
			tn.block_date >= CURRENT_DATE - INTERVAL '{{days}}' day
	),
	-- Get latest prices for each date
	daily_prices AS (
		SELECT
			CAST(timestamp AS DATE) AS price_date,
			blockchain,
			asset_type,
			price_usd,
			ROW_NUMBER() OVER (
				PARTITION BY
					CAST(timestamp AS DATE)
				ORDER BY
					timestamp DESC
			) AS rn
		FROM
			ton.prices_daily
		WHERE
			blockchain = 'ton'
			AND asset_type = 'Jetton'
	),
	position_details AS (
		-- Get position details from execute orders to understand direction, leverage, etc.
		SELECT
			eo.tx_hash,
			eo.user_position,
			eo.trader_addr,
			eo.amm,
			eo.vault,
			eo.vault_token,
			eo.order_direction,
			eo.order_amount,
			eo.order_leverage,
			eo.position_fee, -- Position fee
			eo.position_discount, -- Discount
			eo.position_rebate, -- Rebate
			eo.oracle_price,
			eo.oracle_asset_id,
			eo.block_date AS open_date
		FROM
			stormtrade_ton.execute_order eo
		WHERE
			eo.position_size = 0 -- This indicates a new position being opened
	),
	-- Calculate daily PNL with fee breakdown - using proper scaling
	daily_pnl AS (
		SELECT
			cp.block_date,
			-- Calculate PNL components - scale values to be more readable (multiply by 1e9)
			SUM(cp.realized_pnl * COALESCE(dp.price_usd, 1)) AS pnl_usd,
			SUM(cp.fee_to_stakers * COALESCE(dp.price_usd, 1)) AS borrowing_fee_usd,
			-- Assuming position_fee from the original order can be used as rollover fees
			SUM(pd.position_fee * COALESCE(dp.price_usd, 1)) AS rollover_fee_usd,
			-- PNL before fees = realized PNL + all fees
			SUM(
				(
					cp.realized_pnl + COALESCE(cp.fee_to_stakers, 0) + COALESCE(pd.position_fee, 0)
				) * COALESCE(dp.price_usd, 1)
			) AS pnl_before_fees_usd
		FROM
			closed_positions cp
			LEFT JOIN daily_prices dp ON dp.price_date = cp.block_date
			AND dp.rn = 1
			LEFT JOIN position_details pd ON cp.trader_addr = pd.trader_addr
			AND cp.vault = pd.vault
			AND cp.amm = pd.amm
		GROUP BY
			cp.block_date
	),
	-- Calculate cumulative PNL over time
	cumulative_pnl AS (
		SELECT
			block_date,
			pnl_usd,
			borrowing_fee_usd,
			rollover_fee_usd,
			pnl_before_fees_usd,
			SUM(pnl_usd) OVER (
				ORDER BY
					block_date
			) AS cumulative_pnl_usd,
			SUM(pnl_before_fees_usd) OVER (
				ORDER BY
					block_date
			) AS cumulative_before_fees_usd
		FROM
			daily_pnl
	)
	-- Final query for visualization
SELECT
	block_date,
	pnl_before_fees_usd AS "PNL before Fees",
	borrowing_fee_usd AS "Borrowing Fee",
	rollover_fee_usd AS "Rollover Fee",
	cumulative_before_fees_usd AS "Cumulative before",
	cumulative_pnl_usd AS "Cumulative"
FROM
	cumulative_pnl
ORDER BY
	block_date