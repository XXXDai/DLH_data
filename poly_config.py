POLYMARKET_EVENT_TEMPLATES = [
    "https://polymarket.com/event/{name}-up-or-down-on-{month}-{day}",  # Polymarket日事件模板，字符串
    "https://polymarket.com/event/{symbol}-updown-4h-{epoch_4h}",  # Polymarket四小时事件模板，字符串
    "https://polymarket.com/event/{name}-up-or-down-{month}-{day}-{hour12}{ampm}-et",  # Polymarket小时事件模板，字符串
    "https://polymarket.com/event/{symbol}-updown-15m-{epoch_15m}",  # Polymarket十五分钟事件模板，字符串
    "https://polymarket.com/event/{symbol}-updown-5m-{epoch_5m}",  # Polymarket五分钟事件模板，字符串
    "https://polymarket.com/event/what-price-will-bitcoin-hit-in-february-2026",  # Polymarket固定事件模板，字符串
]  # Polymarket事件模板列表，个数
POLYMARKET_ASSET_TAGS = [
    "bitcoin:btc:1",  # 比特币事件标签，字符串
    "ethereum:eth:0",  # 以太坊事件标签，字符串
    "solana:sol:0",  # 索拉纳事件标签，字符串
    "xrp:xrp:0",  # 瑞波事件标签，字符串
]  # Polymarket资产标签列表，个数
POLYMARKET_TZ_NAME = "America/New_York"  # Polymarket时区名称，时区
