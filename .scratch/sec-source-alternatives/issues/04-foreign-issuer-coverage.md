# 04 — Foreign issuer coverage

**What to build:**

對 TSM、SONY、BABA 等 foreign issuer，先嘗試 SEC 20-F/CIK facts；若 tag 或 filing 不足，明確標示 `UNAVAILABLE` 並保留原因，不能用 Yahoo 財報或未驗證估算取代。

**Blocked by:** 03 — Financial source router + FMP circuit breaker.

**Status:** completed

- [ ] foreign issuer CIK/filing mapping 可測試
- [ ] IFRS/company-specific tags 不完整時 fail closed 並輸出 observable reason
- [ ] 至少一個 SEC foreign issuer fixture 與一個 unsupported fixture
- [ ] 不會污染既有美股 normalized cache
