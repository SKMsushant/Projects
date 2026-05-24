# AlgoTrade GRU ⚡

A high-performance algorithmic trading dashboard featuring a pre-trained generalized GRU recurrent neural network and dynamic on-the-fly transfer learning. The app supports equities and indices from the Indian National Stock Exchange (NSE) and US NASDAQ.

---

## 📂 Project Architecture

```
D:\D\adsw\DSA\Projects\project_algo_trade\
├── data\                        # Historic dataset storage
├── models\                      # Saved model weights & scalers
│   ├── generalized_gru.h5       # Pre-trained generalized baseline model
│   └── generalized_scaler.pkl   # Fitted unified MinMaxScaler
├── src\                         # Modular Python Packages
│   ├── __init__.py
│   ├── data_collection.py       # Basic downloads, header cleaning, deduplication (OHLCV)
│   ├── data_preprocessing.py    # Dynamic indicator generation (EMA, RSI, MACD, Volatility)
│   ├── model.py                 # Core GRU compilation, training, and transfer learning APIs
│   └── pretrain.py              # Diverse asset pre-training executor
├── app.py                       # Premium Streamlit GUI Dashboard
├── requirements.txt             # Python Package dependencies
└── README.md                    # Developer Guide
```

---

## ⚙️ How It Works

### 1. Data Collection & Cleansing (`src/data_collection.py`)
Fetches historical stock prices using `yfinance`. Headers are standardized to lowercase, whitespaces/dots are removed, and duplicate records are immediately dropped to return a clean base OHLCV structure.

### 2. Feature Engineering on the Fly (`src/data_preprocessing.py`)
Technical and volatility features are calculated dynamically:
- **Exponential Moving Average**: EMA-12, EMA-26
- **Relative Strength Index**: RSI-14
- **MACD**: Signal, MACD Line
- **Bollinger Bands**: Upper & Lower (20-day window)
- **Volatility**: Integrates exchange-level volatility indices (`^VIX` or `^INDIAVIX`), falling back to a rolling annualized log returns standard deviation.
- **Normalization**: Normalized using the fitted generalized `MinMaxScaler` object.

### 3. Deep Learning Core (`src/model.py`)
Combines sequential standard GRU layers with dropout adapters:
- Sequence Lookback Window: **60 trading days**
- **Transfer Learning Protocol**: When custom stock datasets are uploaded or dynamically fetched, the base GRU layers can be frozen, letting you train only the dense adapter layers at micro-learning rates (`1e-4` to `1e-5`).

---

## 🚀 Execution & Deployment

### 1. Pre-training the Model (Completed)
The generalized model has already been successfully trained on a diversified asset class (RELIANCE, AAPL, NIFTY50, DJI) and saved. To re-run pre-training at any point, execute:
```bash
..\proj_env\Scripts\python.exe -m src.pretrain
```

### 2. Running the Streamlit Web Application
To spin up the premium Streamlit interactive dashboard, run:
```bash
..\proj_env\Scripts\streamlit.exe run app.py
```

### 3. Ticker Selection Guidelines
- **NSE Equities & Indices**: Append the suffix `.NS` (e.g. `RELIANCE.NS`, `INFY.NS`, `^NSEI` for NIFTY50, `^NSEBANK` for BankNifty).
- **NASDAQ Equities & Indices**: Search as-is (e.g. `AAPL`, `MSFT`, `^DJI` for Dow Jones).

---

## 🏆 Key App Capabilities
- **Markets Overview Panel**: Plotly interactive charts with candle representations and Bollinger Band envelopes.
- **Backtesting Simulator**: Compare algorithmic signals directly against standard passive buy-and-hold strategies.
- **Dynamic Training Dashboard**: Click and watch standard epochs run via a live updating line loss chart and progress gauges, then download the fine-tuned custom weights file directly from the browser.
