# 07 — Independent third-party read-only review

**What to build:**

由獨立、唯讀 reviewer 驗證整個 source replacement release，並在發現問題時交回修復清單；主線修復後 reviewer 必須重新驗證。

**Blocked by:** 06 — Watcher export + R2 end-to-end smoke.

**Status:** completed

- [ ] 需求完整性審查
- [ ] 邏輯正確性及 source provenance 審查
- [ ] 邊界情況審查
- [ ] 程式碼質量及測試覆蓋審查
- [ ] 實際運行、Action、R2 side effects 審查
- [ ] blocker 修復後提交復驗結論
