# 06 — Watcher export + R2 end-to-end smoke

**What to build:**

以 SEC-backed valuation/financials 產生既有 watcher manifest、valuation files、financial files，完成 schema validation 與 sample-symbol smoke；只有完整 release 才能更新 R2 current pointer。

**Blocked by:** 05 — Daily workflow budget/release gate.

**Status:** completed

- [ ] sample symbols export 欄位、source provenance、filing date 正確
- [ ] 12 季上限、null/負 EPS 或 FCF、日期排序 regression tests 通過
- [ ] manifest 不包含失敗或 unavailable ticker
- [ ] R2 release prefix/current pointer 原子性可由 dry-run 驗證
