# 02 — Price source adapter + retired-symbol registry

**What to build:**

保留 Yahoo 作為價格 source，加入可測試的 price adapter、有限 fallback、退市 symbol registry，以及 `SQ` 對現行 `XYZ` 的相容 alias；價格 source 失敗時要清楚回報 unavailable。

**Blocked by:** None — can start immediately.

**Status:** completed

- [ ] Yahoo alias/fallback 不會改變財報 cache key
- [ ] `SQ` 使用 `XYZ` price lookup；`WBA` 等退市 symbol 明確拒絕
- [ ] timeout、空 response、invalid columns、fallback exhaustion 有 fixture tests
- [ ] 不引入第二個需要 API key 的無限重試 source
