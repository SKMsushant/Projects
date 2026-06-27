# Quant Engine: Attention-Based Transformer Stock Predictor ⚡

A premium, high-performance quantitative trading dashboard featuring an attention-based Transformer Encoder stock predictor core. The application leverages sinusoidal positional embeddings, relevance-weighted daily NLP news sentiment integration, and Bayesian hyperparameter optimization to predict multi-step stock prices.

---

## 📂 Project Architecture

```
D:\D\adsw\DSA\Projects\project_algo_trade\
├── reports/                         # Academic documentation & presentation slides
│   ├── research_report.docx         # Full master thesis/research report
│   └── presentation.pptx            # Thesis presentation slide deck
├── analysis.ipynb                   # Quantitative analysis, backtesting, & stats notebook
├── best_hps.json                    # Saved optimal hyperparameters from Keras-Tuner
├── best_transformer_weights.weights.h5 # Pre-trained Transformer Encoder model weights
├── collection_data.py               # Ingestion of historical OHLCV & relevance-weighted news sentiment
├── main_app.py                      # Streamlit interactive dashboard client (UI)
├── model.py                         # Transformer layers and Keras-Tuner training pipeline
├── preparation_data.py              # On-the-fly technical indicator calculations
├── preprocessing_data.py            # Dual-partition sequence generation & symbol MinMaxScaler mapping
├── update_model.py                  # Model weights updater script
├── run_app.bat                      # Batch script to spin up virtual environment and Streamlit
├── requirements.txt                 # Python package dependencies
└── README.md                        # Developer guide
```

---

## ⚙️ How It Works

### 1. Data Ingestion & News Parsing (`collection_data.py`)
* Programmatically downloads historical asset prices using `yfinance`.
* Connects to the Alpha Vantage News Sentiment API to ingest daily financial journalism headlines and summaries.
* Runs a local VADER compound sentiment analyzer and aggregates scores using a **Relevance-Weighted daily formula**:
  $$S_{weighted}(t) = \frac{\sum (Sentiment_i \times Relevance_i)}{\sum Relevance_i}$$
* Incorporates a return-based mock sentiment fallback generator to ensure training pipeline continuity if API limits are reached.

### 2. Feature Engineering & Preprocessing (`preparation_data.py` & `preprocessing_data.py`)
* **Technical Indicators**: Calculates 10 core features on-the-fly: EMA-3, EMA-5, MACD, MACD Signal, RSI-14, Bollinger Bands (Upper, Mid, Lower), VWAP, and MFI-14.
* **Lifetime Expanding Features**: Injects cumulative returns, expanding max (resistance), min (support), and expanding standard deviation of returns computed from inception (Day 1) to anchor long-term trends.
* **Dual Preprocessing Copies**: Generates two distinct tensors using an independent per-symbol `MinMaxScaler` mapping:
  - **Copy A (Traditional Price-Only)**: 23 features.
  - **Copy B (Sentiment-Enhanced)**: 24 features (used for dashboard predictions).
* **Target-Guided Ordinal Encoding**: Encodes categories (Exchange, Country, Asset, Ownership) based on historical mean returns to ensure linear separability.

### 3. Deep Learning Core (`model.py`)
* Implements a custom Keras-Tuner Bayesian optimization search space using an **Attention-Based Transformer Encoder**:
  - **Sequence Lookback Window**: 10 trading days.
  - **Static Positional Embedding**: Sine and cosine spatial encodings to retain temporal order.
  - **Multi-Head Self-Attention**: Learns non-linear correlations between technical events and news sentiment.
  - **Stateless Parallelization**: Bypasses recurrent GRU/LSTM memory boundary carry-over corruption.

---

## 🚀 Execution & Deployment

### 1. Model Tuning & Training
The model hyperparameters are optimized and the best weights are saved on disk. To execute tuning and train the network, run:
```bash
..\proj_env\Scripts\python.exe model.py
```
To retrain or update the model weights using the best parameters, run:
```bash
..\proj_env\Scripts\python.exe update_model.py
```

### 2. Running the Streamlit Web Application
To spin up the premium Streamlit interactive dashboard, execute the batch script:
```bash
run_app.bat
```
Or run the Streamlit CLI command directly:
```bash
..\proj_env\Scripts\streamlit.exe run main_app.py
```

### 3. Ticker Ingest Guidelines
* The configuration panel supports selecting pre-loaded assets (e.g. `RELIANCE`, `TCS`, `NIFTY50`) or custom tickers.
* **Indian Ticker Fallback**: Typing custom Indian stocks (like `RELIANCE` or `sbin`) without suffixes automatically triggers a background fallback to append `.NS` (NSE), updating the exchange to `NSE` and currency to `₹`.

---

## 🏆 Key App Capabilities
* **Interactive Candlesticks**: Displays historical bars with overlays for Bollinger Bands, VWAP, Volume, and RSI, with a forecast boundary marker.
* **Transformer Backtest Performance**: Compare predictions against actuals, and view future prediction path projections.
* **Institution Profile Details**: Displays full legal profile name, business description, sector, industry, and official website using cached metadata.
* **Dynamic Currency Formatting**: Automatically switches between Dollars (`$`) and Rupees (`₹`) in all metrics based on the asset listing exchange.
* **Exchange Off Protection**: Dynamically detects weekends and market holidays (supporting NYSE, NASDAQ, NSE, and BSE calendars) to prevent forecasting on closed market days.
