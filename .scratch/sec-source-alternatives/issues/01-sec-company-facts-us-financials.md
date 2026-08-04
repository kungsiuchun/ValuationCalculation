# 01 — SEC Company Facts US 財報 source

**What to build:**

美國上市公司可以由 SEC Company Facts 取得最近季度財報，轉成 valuation pipeline 使用的 normalized financial records；首次以 AAPL fixture 完成可重現的 revenue、net income、EPS、operating cash flow、capex、shares 與 filing date 路徑。

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] SEC CIK/ticker resolution及 User-Agent、timeout、cache 行為可測試
- [ ] Company Facts facts 可轉成季度 records，日期排序、缺欄位及負值明確處理
- [ ] AAPL fixture test 不需要網絡，並驗證 source、filing date、季度上限
- [ ] API 429/invalid payload fail closed，不使用舊資料冒充 fresh
