# 股票估值帶生成系統 (Stock Valuation Bands Generator)

這是一個基於 Python 的自動化數據管線，專門用於抓取美股財務報表與股價數據，並利用 **混合估值模型 (Hybrid Valuation)** 計算歷史估值區間（PE, P/FCF, P/S）。本系統特別針對 Amazon (AMZN) 等指標波動巨大的公司進行了 **中位數策略** 與 **百分位剪枝** 優化。

## 🚀 腳本核心流程 (Pipeline Decomposition)

本腳本遵循數據工程中的 **ETL (Extract, Transform, Load)** 架構：

### 1. 數據抽取層 (Extract)
* **多維度抓取**：透過 FMP API 抓取 `income-statement` (損益表)、`cash-flow-statement` (現金流量表) 與 `enterprise-values` (企業價值/股數)。
* **碎片化抓取 (`get_fmp_fragmented`)**：按季度（Q1-Q4）循環請求，確保獲取完整的歷史財報。
* **智能緩存機制**：
    * 數據存儲於 `data/fmp_cache/{TICKER}/` 目錄下。
    * **有效期驗證**：緩存有效期為 7 天。若未過期則直接讀取本地 JSON，節省 API 配額並加速執行；若過期則重新發起 API 請求。

### 2. 數據轉換層 (Transform)
* **TTM 計算 (`build_quarterly_ttm`)**：將損益表、現金流量表與企業價值表按日期合併。
* **滾動指標計算**：利用滾動窗口（Rolling Window）計算 **TTM (Trailing Twelve Months)** 指標，產出 `eps_ttm` (每股盈餘)、`fcf_ps_ttm` (每股自由現金流) 與 `sales_ps_ttm` (每股營收)。

### 3. 金融邏輯層 (Core Valuation Logic)
這是腳本的核心 (`calculate_bands`)，結合了量化分析師的專業處理邏輯：
* **對齊與調整因子 (`adj_ratio`)**：計算股價與財報指標的對齊因子，確保即使在拆股後，歷史財務數據與當前股價仍處於同一量級。
* **百分位剪枝 (Percentile Approach)**：計算歷史 $5\%$ 到 $95\%$ 的倍數區間。這會自動剪掉極端的異常值（如 AMZN 因 FCF 極低而產生的上千倍倍數），使圖表顯示比例恢復正常。
* **策略自動切換**：
    * **一般模式**：對於穩定公司使用滾動平均值 (Rolling Mean)。
    * **AMZN/高噪模式**：當數據缺失率高或針對特定個股時，切換至滾動中位數 (Rolling Median)，以對抗轉向盈利期間的數值噪音。
* **帶狀穩定器 (Band Stabilizer)**：將標準差上限限制在均值的 50% 以內，防止估值帶在市場極度動盪時產生誤導性的擴張。
* **強制歸零邏輯**：當基礎財務指標（如 FCF）為負數時，所有估值線自動歸零，避免產生物理意義不明的負數估值帶。

### 4. 輸出層 (Load)
* **數據精簡**：僅導出 2021 年以後的數據點，有效優化前端加載與渲染速度。
* **JSON 安全化 (`clean_nans`)**：遞迴掃描數據結構，將 Python 的 `NaN` 轉換為 JSON 格式支持的 `null`，確保前端圖表庫（如 ECharts）能正確解析。


## 📁 目錄結構
```text
.
├── generate_valuation_final.py  # 核心腳本
└── data/
    ├── fmp_cache/               # 原始財報緩存 (JSON 格式)
    └── results/                 # 最終估值結果
        └── {TICKER}/
            └── valuation_summary.json  # 前端渲染調用的最終文件