# 03 — Financial source router + FMP circuit breaker

**What to build:**

valuation pipeline 先用 SEC normalized financials；SEC 成功時不呼叫 FMP。只有明確支援且未有 SEC facts 時，才進入受控 fallback；任何 source 429、stale payload 或 quota exhaustion 必須停止 ticker，不能生成 fresh-looking output。

**Blocked by:** 01 — SEC Company Facts US 財報 source; 02 — Price source adapter + retired-symbol registry.

**Status:** completed

- [ ] SEC-first routing 可由 source provenance 驗證
- [ ] FMP 不再三 key 連環 retry；429 有 circuit breaker
- [ ] source freshness、payload dates、filing dates 寫入 normalized contract
- [ ] router fixture tests 覆蓋 SEC success、fallback、429、stale、unsupported
