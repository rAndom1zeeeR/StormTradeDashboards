table: stormtrade_ton.update_position
schema:
| name | type |
| --- | --- |
| block_date | date |
| tx_hash | string |
| trace_id | string |
| tx_now | integer |
| tx_lt | long |
| user_position | string |
| vault | string |
| vault_token | string |
| amm | string |
| direction | long |
| origin_op | long |
| oracle_price | uint256 |
| stop_trigger_price | uint256 |
| take_trigger_price | uint256 |
| position_size | int256 |
| position_direction | long |
| position_margin | uint256 |
| position_open_notional | uint256 |
| position_last_updated_cumulative_premium | int256 |
| position_fee | long |
| position_discount | long |
| position_rebate | long |
| position_last_updated_timestamp | long |
| quote_asset_reserve | uint256 |
| quote_asset_weight | uint256 |
| base_asset_reserve | uint256 |
| total_long_position_size | uint256 |
| total_short_position_size | uint256 |
| open_interest_long | uint256 |
| open_interest_short | uint256 |

table: stormtrade_ton.execute_order
schema:
| name | type |
| --- | --- |
| block_date | date |
| tx_hash | string |
| trace_id | string |
| tx_now | integer |
| tx_lt | long |
| user_position | string |
| vault | string |
| vault_token | string |
| amm | string |
| direction | long |
| order_index | long |
| trader_addr | string |
| prev_addr | string |
| ref_addr | string |
| executor_index | long |
| order_type | string |
| order_expiration | long |
| order_direction | long |
| order_amount | uint256 |
| order_triger_price | uint256 |
| order_leverage | uint256 |
| order_limit_price | uint256 |
| order_stop_price | uint256 |
| order_stop_triger_price | uint256 |
| order_take_triger_price | uint256 |
| position_size | int256 |
| position_direction | long |
| position_margin | uint256 |
| position_open_notional | uint256 |
| position_last_updated_cumulative_premium | int256 |
| position_fee | long |
| position_discount | long |
| position_rebate | long |
| position_last_updated_timestamp | long |
| oracle_price | uint256 |
| oracle_spread | uint256 |
| oracle_timestamp | long |
| oracle_asset_id | long |

table: stormtrade_ton.complete_order
schema:
| name | type |
| --- | --- |
| block_date | date |
| tx_hash | string |
| trace_id | string |
| tx_now | integer |
| tx_lt | long |
| user_position | string |
| vault | string |
| vault_token | string |
| amm | string |
| order_type | string |
| order_index | long |
| direction | long |
| origin_op | long |
| oracle_price | uint256 |
| position_size | int256 |
| position_direction | long |
| position_margin | uint256 |
| position_open_notional | uint256 |
| position_last_updated_cumulative_premium | int256 |
| position_fee | long |
| position_discount | long |
| position_rebate | long |
| position_last_updated_timestamp | long |
| quote_asset_reserve | uint256 |
| quote_asset_weight | uint256 |
| base_asset_reserve | uint256 |
| total_long_position_size | uint256 |
| total_short_position_size | uint256 |
| open_interest_long | uint256 |
| open_interest_short | uint256 |

table: stormtrade_ton.trade_notification
schema:
| name | type |
| --- | --- |
| block_date | date |
| tx_hash | string |
| trace_id | string |
| tx_now | integer |
| tx_lt | long |
| vault | string |
| vault_token | string |
| amm | string |
| asset_id | long |
| free_amount | int256 |
| locked_amount | int256 |
| exchange_amount | int256 |
| withdraw_locked_amount | uint256 |
| fee_to_stakers | uint256 |
| withdraw_amount | uint256 |
| trader_addr | string |
| origin_addr | string |
| referral_amount | uint256 |
| referral_addr | string |
| split_executor_reward | long |
| executor_amount | uint256 |
| executor_index | long |

table example: stormtrade_ton.execute_order
| block_date | tx_hash | trace_id | tx_now | tx_lt | user_position | vault | vault_token | amm | direction | order_index | trader_addr | prev_addr | ref_addr | executor_index | order_type | order_expiration | order_direction | order_amount | order_triger_price | order_leverage | order_limit_price | order_stop_price | order_stop_triger_price | order_take_triger_price | position_size | position_direction | position_margin | position_open_notional | position_last_updated_cumulative_premium | position_fee | position_discount | position_rebate | position_last_updated_timestamp | oracle_price | oracle_spread | oracle_timestamp | oracle_asset_id |
| ---------- | -------------------------------------------- | -------------------------------------------- | ---------- | -------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------ | ----------- | ------------------------------------------------------------------ | --------- | ----------- | ------------------------------------------------------------------ | ------------------------------------------------------------------ | ------------------------------------------------------------------ | -------------- | ----------------- | ---------------- | --------------- | ------------ | ------------------- | -------------- | ------------------------ | ----------------- | ---------------------------------- | ----------------------- | ------------- | ------------------ | --------------- | ---------------------- | ---------------------------------------- | ------------ | ----------------- | --------------- | ------------------------------- | ------------ | ------------- | ---------------- | --------------- |
| 2025-05-30 | cV/SUQXdwG2+XvzSUJy5SPcl1FYuPVWLb2N9tJkhk0g= | 9O6qtxzqgvHR/XNTE/eVqy4kefvTliHr0KLPykRFkzY= | 1748566661 | 57730337000001 | 0:E1E849E94612DD4414A65DC074BB114215C0BEA9F6744A6EC1057706086FB1E7 | 0:33E9E84D7CBEFFF0D23B395875420E3A1ECB82E241692BE89C7EA2BD27716B77 | USDT | 0:82673AF7C72BD7E16AF526ECA4A457C2C74C799CAFB8C03C4590524C3C2DB80B | 0 | 0 | 0:A72629D3166CBA43FECB487AFE402CF6F523684ADDFDD1A81D885B29D8C423E7 | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | 0:3C40853010AED8AD3E22AB1AF993D10D4C36D9558F46DE96237E122D9358DDF7 | 2 | stop_limit_order | 1753730338 | 0 | 25000000000 | 5790821923034234880 | 0 | 0 | 0 | 32467753818720117 | 16341359038 | 0 | 36985891707 | 370736529840 | 3582861901 | 950000 | 50000000 | 100000000 | 1748545863 | 21572818534 | 11965665 | 1748566641 | 29 |
| 2025-05-30 | aVjmiCg+eKS3MDe5WYlRT81WCaHhSClqzP9UCwIJgCw= | NiaBh87yAhzEF2tVEr1jsb84qfQSr7/CXEDwddaF/O0= | 1748568034 | 57730930000001 | 0:8A70D2A971CF1FE3E1BD092BB9D1CF3AD3BF87CCD805592EF586983A7BB126BA | 0:E926764FF3D272C73DDEB836975C5521C025AD68E7919A25094E2DE3198805F1 | TON | 0:241E480E9ADB0CDF40E2E3EC3BB0CF524FB520CCA02B2E292E734817BC291FFB | 0 | 0 | 0:CAADBD42ED905CBF37FE191D1747117AA0329784941E78586ED4F1B54F23169A | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | market_order | 1748568890 | 0 | 100000000 | 4638529564027387904 | 0 | 2097152000000008 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 3324941624 | 500002 | 1748567991 | 10 |
| 2025-05-30 | +P+M3FBYtEGVyuqZHeW4soI6iUz5JtnQFCOUSg6ON+Y= | uoNeg+b55hMHUP3WcHUePSLYiyiO1iltH2w+1lRIJBc= | 1748600136 | 57744879000001 | 0:A8DC00468F0E1CC17098A94ABE067D34C24C38512B31AD2B6802047E971B3DB9 | 0:33E9E84D7CBEFFF0D23B395875420E3A1ECB82E241692BE89C7EA2BD27716B77 | USDT | 0:3BBB2986514CA4229B36D8F8DAEF1F2B5FD0FB3DBB6EBE46843E51C0C3895912 | 0 | 0 | 0:75717FA91E4039A1739B05AD6D6D6A393B1B907DACB10E5E2F8CDEFE9B2E3CCD | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | market_order | 1748600965 | 0 | 13700000000 | 5778973014234234880 | 0 | 0 | 902441861124 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 3299628000000 | 706000000 | 1748600080 | 6 |
| 2025-05-30 | mGVMQzsNfiRqhme6wLqXk7xBwkhozk35R5JnMNMldZw= | gRwyb196oPHA0uYyq+s50DCZweBg+8QAkKARIVrbLrw= | 1748635664 | 57759873000001 | 0:796952C354B5468CADEA720FC38FCB589FC2C20425E509B48CB83BA2CD301969 | 0:E926764FF3D272C73DDEB836975C5521C025AD68E7919A25094E2DE3198805F1 | TON | 0:241E480E9ADB0CDF40E2E3EC3BB0CF524FB520CCA02B2E292E734817BC291FFB | 1 | 0 | 0:384D1084944394AB1D7F34707CB86E653372FF50E7A93CCB15D130154AD733D4 | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | market_order | 1748636520 | 1 | 1000000000 | 4880121474427387905 | 118 | 630823342332 | 52181418681396242 | 0 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 3314997175 | 0 | 1748635627 | 10 |
| 2025-05-30 | dwy2BSZ36X+kzqkbLOllQu4BItsOPSYmRr2PymTvEQ8= | pe90n0vqlogS7fhqU/Mw6Tf1Jst5MaerMJQMNHILo2k= | 1748646107 | 57764322000001 | 0:FAF3D6BF074544F8A3D2E122B201899D0DF47E701B6B58E96680F2CA96106DDF | 0:E926764FF3D272C73DDEB836975C5521C025AD68E7919A25094E2DE3198805F1 | TON | 0:4483933461AF7DF324A9C9AB1A33181DBCF0136B5BCEE8F9D1DB7ABF31FAEF08 | 0 | 0 | 0:822F67577520B29FC05A2F0ED331D619C27C5D65BFEA6880B160AB7FD15A3A68 | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | stop_limit_order | 1753828403 | 0 | 5000000000 | 5769850403034234880 | 0 | 0 | 0 | 12168352 | 582785590953 | 0 | 11641733862 | 552214225578 | 60582259 | 1800000 | 0 | 0 | 1748644382 | 895737307 | 100026 | 1748646088 | 19 |
| 2025-05-31 | ERQwRb0UtsZt6gR7ZrfotT8wtYLW/RzdxanpuBjcRGI= | 93wtYrLCNeF3HbOwI6mohllkkJqjc2JtQID7OES3Sk4= | 1748693981 | 57784863000001 | 0:E0D5EF63D41B41D914A523B5D208E94C2C4873B9A1C7B2DC522C4EBB80240180 | 0:E926764FF3D272C73DDEB836975C5521C025AD68E7919A25094E2DE3198805F1 | TON | 0:1D778751FC2055494FE343EE26053BD95DFAE694CC213212E8879093877450A8 | 0 | 1 | 0:1EF4C5C5B4799758FF442DF7002AB121925F2A68ED326C0D061A836C5C724EF1 | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | take_profit_order | 0 | 0 | 72288128930 | 0 | 72288128927 | 0 | 1851851852 | 226338450018 | 79032793 | 1800000 | 0 | 0 | 1748688543 | 3155822185 | 0 | 1748693943 | 40 |
| 2025-05-31 | cjEYa6b3HLxjCAd8C2b/SAxOKi4aD1lUH2dXXyJXz2Q= | EhV54DT9mLZs4w1wLZuEBC+jlMd9Z+v1NGd/xtEy0/Q= | 1748665690 | 57772838000001 | 0:E0D5EF63D41B41D914A523B5D208E94C2C4873B9A1C7B2DC522C4EBB80240180 | 0:E926764FF3D272C73DDEB836975C5521C025AD68E7919A25094E2DE3198805F1 | TON | 0:1D778751FC2055494FE343EE26053BD95DFAE694CC213212E8879093877450A8 | 1 | 0 | 0:1EF4C5C5B4799758FF442DF7002AB121925F2A68ED326C0D061A836C5C724EF1 | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | market_order | 1748666552 | 1 | 1000000000 | 4880121474427387904 | 21515 | 142851608663385434423304 | 0 | 0 | -44405643481 | 1 | 909646183 | 138200179947 | 78868903 | 1800000 | 0 | 0 | 1748665578 | 3129866876 | 100032 | 1748665656 | 40 |
| 2025-05-30 | PqU9v3kSmrGIk3QP7uCqe0eRwxgKmU/VaRsaMYjwpXU= | o3CdGCPF8hKicEt1R2TXJ2R2g7xXYYuu8BtD5MmyodQ= | 1748622824 | 57754477000001 | 0:885948EF21C726D3CE8C607F1C41C556525B2676BD050D7D010E22997AA55F54 | 0:E926764FF3D272C73DDEB836975C5521C025AD68E7919A25094E2DE3198805F1 | TON | 0:182C4862F70FEFFCDC3FBAA102E67FD2F00C189E3514B91C00F446BAC14BEBEC | 0 | 0 | 0:8D80EAAC913D9BA20349971D1C11FCDC5E7FEF90C750FAE81F24A6DF10AEA856 | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | stop_loss_order | 0 | 0 | 400378838802 | 546926000 | 1459278837020 | 0 | 151212941709 | 909447033219 | 20637886 | 1800000 | 0 | 0 | 1748362708 | 0 | 0 | 0 | 0 |
| 2025-05-31 | ZH9HJJwJzXDPxiys/6C1BBuAqDUqPaUssbYByGQ5CeE= | A0LQ7wVpLgbuiOe7zQcO0RphNLGKdxLSuo+KP1IjlBo= | 1748667978 | 57773823000003 | 0:1CB5D9BF6B2E29CDDF7050CB1629D867951FF4703C31A17870083D3EA9077599 | 0:E926764FF3D272C73DDEB836975C5521C025AD68E7919A25094E2DE3198805F1 | TON | 0:241E480E9ADB0CDF40E2E3EC3BB0CF524FB520CCA02B2E292E734817BC291FFB | 0 | 0 | 0:8CFEAFF7A0FE59480617D47F09B61A0284342CF5A847653B57BC0888B2C16F23 | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | 0:ED714DDA29E1EA361EFAE0CB1A6B233A222BF808BA329FD9B9F30E407F3CF29B | 2 | market_order | 1748668836 | 0 | 20000000000 | 5785579043034234880 | 0 | 0 | 0 | 4679096436231859991918972352229116 | 0 | 0 | 0 | 0 | 0 | 0 | 50000000 | 150000000 | 0 | 3070931853 | 75028 | 1748667946 | 10 |
| 2025-05-30 | iPLXodmDiP6qspfIiV1LIPRBoF/oKTrPS2j6hRToLbc= | EaZHOhjt7bgwBTnyFpSthLATv8wUm+bn22st3/GRBYE= | 1748636758 | 57760337000001 | 0:F932F93078D35F9255530FEDA4B58C652A3C98B80E267C136A369D6112C056C8 | 0:33E9E84D7CBEFFF0D23B395875420E3A1ECB82E241692BE89C7EA2BD27716B77 | USDT | 0:0918175E74CCEE1411236E2C48656C27515444B5DD85347558DD5D5E637E9A01 | 0 | 0 | 0:D21A981C7BC45B77B01436A22C723325ACDDF43A2626324BF1ED739C4B13CD0E | 0:763A947D82A6FA9668A29C004B8EDD590C68C757A5CEF52F83AB312FD8E28832 | addr_none | 2 | market_order | 1748637597 | 0 | 933000000000 | 6742928931034234880 | 0 | 0 | 42 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 11275747470 | 252529 | 1748636707 | 63 |
