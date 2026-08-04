# 05 — Daily workflow budget/release gate

**What to build:**

每日 GitHub Action 只做受控 source fetch、cache reuse、tests、export validation；feature branch 不得修改 production R2 lifecycle、current pointer 或 master。任何 source failure 沿用上一個 release，並且 workflow 失敗要有可觀測原因。

**Blocked by:** 03 — Financial source router + FMP circuit breaker; 04 — Foreign issuer coverage.

**Status:** completed

- [ ] workflow 執行 Python tests、schema tests、source budget checks
- [ ] feature branch 所有 R2 write/lifecycle/git push side effects 都 skip
- [ ] master 只在全 ticker source freshness + export validation pass 後 publish
- [ ] source quota/429 不會觸發重複 API spam
