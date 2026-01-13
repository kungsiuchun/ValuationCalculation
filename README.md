# 股票估值帶生成系統 (Stock Valuation Bands Generator)

![Update Stocks Status](https://github.com/kungsiuchun/ValuationCalculation/actions/workflows/update.valuation.yaml/badge.svg)`

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


---

## 🔍 技術細節 Q&A (Technical Deep Dive)

> [!TIP]
> **Q1: 為什麼前端顯示不了 NaN？腳本是如何解決的？**
>
> **A:** 在數據工程中，`NaN` (Not a Number) 是 Python pandas 處理缺失值的標準，但 JSON 格式並不支援它。如果直接輸出，前端圖表庫（如 Highcharts/ECharts）會崩潰。
>
> * **解決方案**：腳本在輸出前會執行 `clean_nans` 遞迴函數，將所有 `NaN` 轉換為 JSON 兼容的 `null`。
> * **效果**：前端會將其視為「數據中斷」而非「程式錯誤」，線條會優雅地斷開或歸零，而不是導致整個網頁報錯。

---

> [!NOTE]
> **Q2: 財務指標是季度更新，但股價是每日更新，兩者如何對齊？**
>
> **A:** 這是透過 **「線性插值 (Linear Interpolation)」** 實現的。
>
> * **邏輯**：如果 Q3 EPS 是 2.1，Q4 是 2.9，腳本不會讓指標在三個月內停留在 2.1 然後突然跳到 2.9（階梯狀）。相反，它會根據日期比例計算每日增量，讓指標平滑地從 2.1 增長到 2.9。
> * **意義**：這消除了財報發布日的「斷層」，讓估值帶 (Valuation Bands) 的移動更加流暢，能更真實地反映股價相對於盈利趨勢的變動。



---

> [!IMPORTANT]
> **Q3: 不同的 Rolling Window (1Y, 3Y, 5Y) 代表什麼意義？**
>
> **A:** 它們代表了不同長度的 **「歷史濾鏡」**。
>
> * **1Y (近期)**：反映市場近期的脾氣，適合快速成長或變動劇烈的個股。
> * **5Y (長期)**：代表公司的價值中樞。
> * **核心邏輯**：不論選擇哪個窗口，財務指標基礎 (`metric_final`) 都是一樣的，改變的是「歷史平均倍數」。這讓投資者能一眼看出目前的股價是處於「近期」還是「長期」的歷史低位。

---

> [!CAUTION]
> **Q4: 估值倍數（Multiple）是從 API 抓取的嗎？**
>
> **A:** **不是**。`yfinance` 僅提供股價，`FMP` 提供財報。
>
> * **合成邏輯**：腳本會拿當天的 `Adj Close` 除以當天插值後的財務指標，自行「合成」出一條每日歷史 PE 曲線。
> * **再加工**：有了這條合成曲線後，才進行 `rolling().mean()` 運算，產出不同窗口的平均倍數。這賦予了腳本極大的靈活性，可以計算任何自定義指標的倍數。

---

> [!NOTE]
> **Q5: 如果 FMP 只有 5 年數據，但 yfinance 抓 10 年，會有什麼影響？**
>
> **A:** 這會產生 **「數據預熱期 (Warm-up Period)」**。
>
> * **現象**：在數據開始的前幾年（例如 2021-2023），5Y Rolling PE 其實是在計算「從數據開始到現在」的平均值，而非真正的「5 年」。
> * **應對**：雖然 10 年股價無法補齊缺失的財報，但它確保了股價基準線的完整。隨著時間推移，當系統運行的時間越久，這條 5Y 線會自動演變成真正的五年平均，無需手動干預。

---

> [!TIP]
> **Q6: 腳本如何處理「拆股 (Stock Splits)」造成的數據斷層？**
>
> **A:** 透過 **`adj_ratio`** 邏輯。
>
> * **原理**：腳本計算 `yfinance` 的 `Adj Close` 與 `Close` 的比例。如果公司發生拆股（如 AMZN 在 2022 年 1 拆 20），這個比例會劇烈變化。
> * **應用**：腳本會將這個比例應用到 FMP 的原始財務指標上，確保 2022 年之前的 EPS 會被自動縮小 20 倍，從而與現在的股價量級完美對齊。

---

## 📁 目錄結構
```text
.
├── generate_valuation_final.py  # 核心腳本
└── data/
    ├── fmp_cache/               # 原始財報緩存 (JSON 格式)
    |    └── {TICKER}/
    |       └──{endpoint}_{q}.json
    └── results/                 # 最終估值結果
        └── {TICKER}/
            └── valuation_summary.json  # 前端渲染調用的最終文件

