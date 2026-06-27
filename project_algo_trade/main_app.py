import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import tensorflow as tf
from tensorflow.keras.models import load_model
import os
import time
import os
import keras_tuner as kt
import json
from PIL import Image
# Import your pre-written custom modules!
from collection_data import Data_collection, stock_tups, stack_company_data
from preparation_data import Data_prep
from preprocessing_data import StatefulDataPreprocessor
from model import Transformer_Encoder, Static_Positional_Embedding, Transformer_Encoder_Model
import yfinance as yf


@st.cache_data
def get_ticker_info(ticker_name):
    try:
        ticker = yf.Ticker(ticker_name)
        info = ticker.info
        return {
            "name": info.get('longName') or info.get('shortName') or ticker_name,
            "sector": info.get('sector', 'N/A'),
            "industry": info.get('industry', 'N/A'),
            "website": info.get('website', 'N/A'),
            "summary": info.get('longBusinessSummary', 'No business summary available.')
        }
    except Exception:
        return {
            "name": ticker_name,
            "sector": "N/A",
            "industry": "N/A",
            "website": "N/A",
            "summary": "No business summary available (API offline)."
        }


def is_market_holiday(dt, exchange):
    # Weekends (Saturday and Sunday) are always off
    if dt.weekday() >= 5:
        return True
        
    month = dt.month
    day = dt.day
    year = dt.year
    
    # Standard static holidays for US markets (NYSE, NASDAQ, GLOBAL)
    if exchange in ['NYSE', 'NASDAQ', 'GLOBAL']:
        # New Year's Day (Jan 1)
        if month == 1 and day == 1: return True
        # Juneteenth (June 19)
        if month == 6 and day == 19: return True
        # Independence Day (July 4)
        if month == 7 and day == 4: return True
        # Christmas Day (Dec 25)
        if month == 12 and day == 25: return True
        
        # MLK Day: Third Monday in Jan
        if month == 1 and dt.weekday() == 0 and 15 <= day <= 21: return True
        # President's Day: Third Monday in Feb
        if month == 2 and dt.weekday() == 0 and 15 <= day <= 21: return True
        # Memorial Day: Last Monday in May
        if month == 5 and dt.weekday() == 0 and day >= 25: return True
        # Labor Day: First Monday in Sep
        if month == 9 and dt.weekday() == 0 and day <= 7: return True
        # Thanksgiving: Fourth Thursday in Nov
        if month == 11 and dt.weekday() == 3 and 22 <= day <= 28: return True
        
    # Standard holidays for Indian markets (NSE, BSE)
    if exchange in ['NSE', 'BSE']:
        # Republic Day (Jan 26)
        if month == 1 and day == 26: return True
        # Ambedkar Jayanti (Apr 14)
        if month == 4 and day == 14: return True
        # Maharashtra Day (May 1)
        if month == 5 and day == 1: return True
        # Independence Day (Aug 15)
        if month == 8 and day == 15: return True
        # Gandhi Jayanti (Oct 2)
        if month == 10 and day == 2: return True
        # Christmas Day (Dec 25)
        if month == 12 and day == 25: return True
        
        # Dynamic holidays for NSE/BSE (2024-2026)
        holiday_strings = [
            # 2024 Indian Holidays
            "2024-03-08", "2024-03-25", "2024-03-29", "2024-04-11", 
            "2024-04-17", "2024-05-01", "2024-06-17", "2024-07-17", 
            "2024-11-01", "2024-11-15",
            # 2025 Indian Holidays
            "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", 
            "2025-04-18", "2025-05-01", "2025-06-06", "2025-09-05", 
            "2025-10-02", "2025-10-20", "2025-11-05",
            # 2026 Indian Holidays
            "2026-03-04", "2026-04-03", "2026-05-01", "2026-01-26",
            "2026-04-14", "2026-08-15", "2026-10-02", "2026-12-25"
        ]
        if dt.strftime("%Y-%m-%d") in holiday_strings:
            return True
            
    return False

def get_valid_trading_days(start_date, end_date, exchange):
    all_days = pd.date_range(start=start_date, end=end_date)
    valid_days = []
    for d in all_days:
        if d.weekday() < 5 and not is_market_holiday(d, exchange):
            valid_days.append(d)
    return pd.DatetimeIndex(valid_days)


st.set_page_config(page_title="Quant-Trading Dashboard", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Space Grotesk', sans-serif;
        background-color: #0d0e12;
        color: #e2e8f0;
    }
    
    .stApp {
        background: radial-gradient(circle at 50% 50%, #1a1c24 0%, #0d0e12 100%);
    }
    
    /* Sleek Card Styling */
    .metric-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 24px;
        backdrop-filter: blur(10px);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-4px);
        border-color: rgba(255, 255, 255, 0.1);
    }
    
    .metric-label {
        font-size: 14px;
        color: #94a3b8;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    .metric-value {
        font-size: 32px;
        font-weight: 700;
        margin-top: 8px;
    }
    
    .text-up { color: #10b981 !important; }
    .text-down { color: #f43f5e !important; }
    .text-neutral { color: #64748b !important; }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_trained_model():
    """Loads the pre-trained stateful GRU model."""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        hps_path = os.path.join(current_dir, "best_hps.json")
        weights_path=os.path.join(current_dir,'best_transformer_weights.weights.h5')

        with open(hps_path,'r') as f:
            hps=json.load(f)

        hp=kt.HyperParameters()
        for k,v in hps.items():
            hp.values[k]=v

        seq_len=10
        num_features=24
        hypermodel=Transformer_Encoder_Model(seq_len=seq_len,num_features=num_features)
        model=hypermodel.build(hp)

        model.load_weights(weights_path)
        return model
    
    except Exception as e:
        st.error(f" Failed to load 'best_stateful_gru.keras'. Ensure you ran model.py first! Error: {e}")
        return None

def reset_model_states(model):
    """Keras 3 safe model state reset."""
    for layer in model.layers:
        if hasattr(layer, 'reset_states'):
            layer.reset_states()

st.sidebar.markdown("##  Configuration Panel")

# Stock list parsing
symbol_options = [tup[0] for tup in stock_tups]
selected_symbol = st.sidebar.selectbox("Select Stacked Asset", options=["Custom Ticker"] + symbol_options)

custom_ticker = ""
if selected_symbol == "Custom Ticker":
    custom_ticker = st.sidebar.text_input("Enter yfinance Ticker (e.g. AAPL, MSFT, ^NSEI)", value="AAPL")

# Date range selectors
start_date = st.sidebar.date_input("Inference Start Date", value=pd.to_datetime("2015-01-01"))
end_date = st.sidebar.date_input("Inference End Date", value=pd.to_datetime("today"))

# Forecast target date configuration
st.sidebar.markdown("---")
st.sidebar.markdown("###  Future Forecast Horizon")
forecast_target_date = st.sidebar.date_input("Forecast Target Date", value=None)

# Upgraded selector: Toggle between Raw GRU and Blended Technical Momentum mode
forecast_mode = st.sidebar.selectbox(
    "Forecast Model Mode",
    options=[
        "🚀 Blended Transformer + Technical Momentum (Dynamic)",
        "🎯 Pure Transformer (Current Static Weights)"
    ]
)

st.markdown("<h1 style='text-align: center; font-weight: 700; margin-bottom: 2px;'>⚡ QUANT ENGINE</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #94a3b8; font-size: 16px; margin-bottom: 30px;'>Attention-Based Transformer Stock Predictor Core</p>", unsafe_allow_html=True)

model = load_trained_model()

if model is not None:
    # 1. Resolve selected ticker and metadata
    if selected_symbol != "Custom Ticker":
        # Pull metadata from stock_tups
        tup = [t for t in stock_tups if t[0] == selected_symbol][0]
        symbol, exchange, country, ownership, asset, ticker_name = tup
    else:
        symbol = custom_ticker.upper()
        # Dynamically resolve exchange and country based on ticker suffix
        if symbol.endswith(".NS") or symbol in ["^NSEI", "^NSEBANK", "^INDIAVIX"]:
            exchange = "NSE"
            country = "India"
        elif symbol.endswith(".BO"):
            exchange = "BSE"
            country = "India"
        else:
            exchange = "GLOBAL"
            country = "Global"
        ownership, asset, ticker_name = "Private", "Equity", custom_ticker
        
    # Dynamically resolve currency symbol based on country
    currency_symbol = "₹" if country.lower() == "india" else "$"
        
    with st.spinner(f"⏳ Syncing real-time market data for {symbol}..."):
        # 2. Ingest latest data
        collector = Data_collection(symbol, exchange, country, ownership, asset, ticker_name)
        df_raw = collector.collect_data(start_date=start_date.strftime('%Y-%m-%d'), end_date=end_date.strftime('%Y-%m-%d'))
        
        # Fallback for Indian stocks (e.g. if user entered RELIANCE without .NS)
        if df_raw.empty and selected_symbol == "Custom Ticker" and not (symbol.endswith(".NS") or symbol.endswith(".BO")):
            fallback_ticker = symbol + ".NS"
            fallback_collector = Data_collection(symbol, "NSE", "India", ownership, asset, fallback_ticker)
            df_fallback = fallback_collector.collect_data(start_date=start_date.strftime('%Y-%m-%d'), end_date=end_date.strftime('%Y-%m-%d'))
            if not df_fallback.empty:
                df_raw = df_fallback
                exchange = "NSE"
                country = "India"
                currency_symbol = "₹"
                ticker_name = fallback_ticker
        
    if not df_raw.empty:
        # Guarantee datetime format for type safety
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        
        # 3. Feature engineering
        prep = Data_prep()
        df_prep = prep.prepare_features(df_raw)
        
    
        # 4. Target-Guided Preprocessing (Generates both copies, trains Streamlit models on Copy B)
        preprocessor = StatefulDataPreprocessor(seq_length=10, target_col='close')
        (seq_a, df_scaled_a), (sequences, df_scaled) = preprocessor.fit_transform_dual(df_prep)
        
        X_all, y_all = sequences[symbol]
        
        # Dynamically align feature dimensions to prevent input shape mismatches
        model_features = model.input_shape[2]
        if X_all.shape[2] != model_features:
            X_all = X_all[:, :, :model_features]
            
        # Chronological train-test split (80% / 20%) to match training specs
        split_idx = int(len(X_all) * 0.8)
        X_val, y_val = X_all[split_idx:], y_all[split_idx:]
        
        # Get target index (close price) and scaler
        target_idx = preprocessor.feature_cols.index('close')
        scaler = preprocessor.symbol_scalers[symbol]
        
        
        last_actual_date = pd.to_datetime(df_raw['date'].iloc[-1])
        target_dt = pd.to_datetime(forecast_target_date) if forecast_target_date is not None else None
        
        # Check if the target date is valid
        is_active = True
        off_message = ""
        
        if forecast_target_date is None:
            is_active = False
            off_message = "NO TARGET DATE SELECTED"
        elif is_market_holiday(target_dt, exchange):
            is_active = False
            off_message = "EXCHANGE OFF"
        elif target_dt <= last_actual_date:
            is_active = False
            off_message = "INVALID FORECAST DATE (PAST/TODAY)"
            
        # Define default metrics
        today_actual_real = 0.0
        target_pred_real = 0.0
        price_diff = 0.0
        pct_change = 0.0
        directional_acc = 0.0
        forecast_label = "OFF" if off_message == "EXCHANGE OFF" else "N/A"
        pct_text = "0.00%"
        direction_class = ""
        
        if is_active:
            # Identify valid trading days to forecast (excluding weekends and holidays!)
            future_bdays = get_valid_trading_days(last_actual_date + pd.Timedelta(days=1), target_dt, exchange)
            k_steps = len(future_bdays)
            
            # Calculate Tomorrow's Price prediction (Standard Baseline)
            latest_seq = X_all[-1:] # Shape: (1, 10, 23)
            reset_model_states(model)
            tomorrow_pred_scaled = model.predict(latest_seq, batch_size=1, verbose=0)[0, 0]
            
            dummy_pred = np.zeros((1, len(scaler.scale_)))
            dummy_pred[0, target_idx] = tomorrow_pred_scaled
            tomorrow_pred_real = scaler.inverse_transform(dummy_pred)[0, target_idx]
            
            today_actual_real = df_prep['close'].iloc[-1]
            
            if k_steps <= 1:
                k_steps = 1
                forecast_label = "Tomorrow"
                target_pred_real = tomorrow_pred_real
                future_preds = [(last_actual_date + pd.Timedelta(days=1), tomorrow_pred_real)]
            else:
                forecast_label = f"{target_dt.strftime('%d-%b-%Y')}"
                
                if "Blended" in forecast_mode:
                    # DYNAMIC MODE: Blended Technical Momentum Interpolation
                    recent_rsi = df_prep['rsi_14'].iloc[-1]
                    recent_macd = df_prep['macd'].iloc[-1]
                    recent_macd_sig = df_prep['macd_signal'].iloc[-1]
                    bb_high = df_prep['bollinger_hband'].iloc[-1]
                    bb_low = df_prep['bollinger_lband'].iloc[-1]
                    
                    drift = 0.0
                    if recent_rsi > 55 or recent_macd > recent_macd_sig:
                        drift += 0.0018 * (recent_rsi / 50.0)
                    elif recent_rsi < 45 or recent_macd < recent_macd_sig:
                        drift -= 0.0018 * ((100.0 - recent_rsi) / 50.0)
                    
                    if today_actual_real > bb_high * 0.96:
                        drift -= 0.001
                    elif today_actual_real < bb_low * 1.04:
                        drift += 0.001
                    
                    future_preds = []
                    for step in range(k_steps):
                        fut_date = future_bdays[step]
                        pred_real = tomorrow_pred_real * (1.0 + drift * step)
                        future_preds.append((fut_date, pred_real))
                        
                    target_pred_real = future_preds[-1][1]
                    
                else:
                    # STATIC MODE: Pure Transformer Autoregressive Multi-step Prediction
                    df_forecast = df_raw.copy().sort_values('date').reset_index(drop=True)
                    rolling_avg_volume = df_raw['volume'].iloc[-10:].mean()
                    future_preds = []
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for step in range(k_steps):
                        fut_date = future_bdays[step]
                        status_text.text(f"🔮 Transformer forecasting step {step + 1}/{k_steps}...")
                        
                        df_prep_temp = prep.prepare_features(df_forecast)
                        df_lifetime = preprocessor._add_lifetime_expanding_features(df_prep_temp)
                        df_encoded = preprocessor.transform_categorical(df_lifetime)
                        
                        exclude_cols = ['date', 'symbol'] + preprocessor.categorical_cols
                        numerical_cols = [col for col in df_encoded.columns if col not in exclude_cols]
                        df_scaled_temp = preprocessor.scale_numerical_per_symbol(df_encoded, numerical_cols, is_training=False)
                        
                        latest_seq_df = df_scaled_temp[preprocessor.feature_cols].iloc[-10:]
                        X_latest = latest_seq_df.values[np.newaxis, :, :model_features]
                        
                        reset_model_states(model)
                        pred_scaled = model.predict(X_latest, batch_size=1, verbose=0)[0, 0]
                        
                        dummy_temp = np.zeros((1, len(scaler.scale_)))
                        dummy_temp[0, target_idx] = pred_scaled
                        pred_real = scaler.inverse_transform(dummy_temp)[0, target_idx]
                        
                        future_preds.append((fut_date, pred_real))
                        
                        new_row = {
                            'date': fut_date, 'open': pred_real, 'high': pred_real, 'low': pred_real, 'close': pred_real,
                            'volume': rolling_avg_volume, 'symbol': symbol, 'exchange': df_raw['exchange'].iloc[-1],
                            'country': df_raw['country'].iloc[-1], 'ownership_type': df_raw['ownership_type'].iloc[-1],
                            'asset_type': df_raw['asset_type'].iloc[-1]
                        }
                        df_forecast = pd.concat([df_forecast, pd.DataFrame([new_row])], ignore_index=True)
                        progress_bar.progress((step + 1) / k_steps)
                        
                    progress_bar.empty()
                    status_text.empty()
                    target_pred_real = future_preds[-1][1]
                    
            price_diff = target_pred_real - today_actual_real
            pct_change = (price_diff / today_actual_real) * 100
            arrow = "▲" if price_diff >= 0 else "▼"
            pct_text = f"{arrow} {pct_change:+.2f}%"
            direction_class = "text-up" if price_diff >= 0 else "text-down"
            
            # Predict Validation Set
            reset_model_states(model)
            val_preds_scaled = model.predict(X_val, batch_size=1, verbose=0)
            
            dummy_val_preds = np.zeros((len(val_preds_scaled), len(scaler.scale_)))
            dummy_val_preds[:, target_idx] = val_preds_scaled.flatten()
            val_preds_real = scaler.inverse_transform(dummy_val_preds)[:, target_idx]
            
            dummy_val_actuals = np.zeros((len(y_val), len(scaler.scale_)))
            dummy_val_actuals[:, target_idx] = y_val
            val_actuals_real = scaler.inverse_transform(dummy_val_actuals)[:, target_idx]
            
            val_dates = df_prep['date'].iloc[split_idx + 10:].values
            
            today_val_actuals = []
            for seq in X_val:
                today_val_actuals.append(seq[-1, target_idx])
                
            dummy_today = np.zeros((len(today_val_actuals), len(scaler.scale_)))
            dummy_today[:, target_idx] = today_val_actuals
            today_val_real = scaler.inverse_transform(dummy_today)[:, target_idx]
            
            actual_direction = np.sign(val_actuals_real - today_val_real)
            predicted_direction = np.sign(val_preds_real - today_val_real)
            directional_acc = np.mean(actual_direction == predicted_direction) * 100
            
        # Fetch and render the institution name/header
        info_data = get_ticker_info(ticker_name)
        st.markdown(f"""
            <div style="text-align: center; margin-top: -15px; margin-bottom: 25px;">
                <span style="font-size: 28px; font-weight: 700; color: #60a5fa; letter-spacing: 0.5px; font-family: 'Space Grotesk', sans-serif;">{info_data['name']}</span>
                <span style="font-size: 20px; font-weight: 600; color: #94a3b8; margin-left: 8px; font-family: 'Space Grotesk', sans-serif;">({symbol})</span>
            </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Last Closed Price</div>
                    <div class="metric-value">{currency_symbol}{today_actual_real:,.2f}</div>
                </div>
            """, unsafe_allow_html=True)
            
        with col2:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Forecast ({forecast_label})</div>
                    <div class="metric-value">{currency_symbol}{target_pred_real:,.2f}</div>
                </div>
            """, unsafe_allow_html=True)
            
        with col3:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Projected Shift</div>
                    <div class="metric-value {direction_class}">{pct_text}</div>
                </div>
            """, unsafe_allow_html=True)
            
        with col4:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Transformer Directional Accuracy</div>
                    <div class="metric-value" style="color: #60a5fa;">{directional_acc:.1f}%</div>
                </div>
            """, unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        if is_active:
            tab1, tab2, tab3 = st.tabs([
                "📈 Market Candlestick & Indicators", 
                "🧠 Transformer Model Backtest Analysis",
                "🏢 Institution Details"
            ])
            
            # Connect seamless forecasting boundary coordinates
            proj_dates_connected = [df_prep['date'].iloc[-1]] + [p[0] for p in future_preds]
            proj_prices_connected = [df_prep['close'].iloc[-1]] + [p[1] for p in future_preds]
            
            with tab1:
                # Multi-Subplot Chart: Candlestick/BB/VWAP + Volume + Oscillators
                fig = make_subplots(
                    rows=3, cols=1, 
                    shared_xaxes=True, 
                    vertical_spacing=0.03, 
                    row_heights=[0.6, 0.2, 0.2]
                )
                
                # Subplot 1: Candlesticks (Historical Only)
                fig.add_trace(go.Candlestick(
                    x=df_prep['date'], open=df_prep['open'], high=df_prep['high'], low=df_prep['low'], close=df_prep['close'],
                    name="OHLC (Historical)", showlegend=True
                ), row=1, col=1)
                
                # Subplot 1: Future Prediction Continuation Line (Neon Blue Dashed)
                fig.add_trace(go.Scatter(
                    x=proj_dates_connected, y=proj_prices_connected, 
                    name="Transformer Forecast Path", 
                    line=dict(color="#60a5fa", width=3, dash="dash")
                ), row=1, col=1)
                
                # Subplot 1 Overlays: VWAP & Bollinger Bands
                fig.add_trace(go.Scatter(x=df_prep['date'], y=df_prep['vwap'], name="VWAP", line=dict(color="#f59e0b", width=1.5)), row=1, col=1)
                fig.add_trace(go.Scatter(x=df_prep['date'], y=df_prep['bollinger_hband'], name="BB Upper", line=dict(color="rgba(96, 165, 250, 0.3)", dash="dash")), row=1, col=1)
                fig.add_trace(go.Scatter(x=df_prep['date'], y=df_prep['bollinger_lband'], name="BB Lower", line=dict(color="rgba(96, 165, 250, 0.3)", dash="dash"), fill='tonexty'), row=1, col=1)
                
                # Subplot 2: Volume
                volume_colors = ['#10b981' if df_prep['close'].iloc[i] >= df_prep['open'].iloc[i] else '#f43f5e' for i in range(len(df_prep))]
                fig.add_trace(go.Bar(x=df_prep['date'], y=df_prep['volume'], name="Volume", marker_color=volume_colors, showlegend=False), row=2, col=1)
                
                # Subplot 3: RSI Indicator
                fig.add_trace(go.Scatter(x=df_prep['date'], y=df_prep['rsi_14'], name="RSI (14)", line=dict(color="#a855f7", width=1.5)), row=3, col=1)
                fig.add_trace(go.Scatter(x=df_prep['date'], y=[70]*len(df_prep), name="Overbought", line=dict(color="rgba(244, 63, 94, 0.5)", dash="dot"), showlegend=False), row=3, col=1)
                fig.add_trace(go.Scatter(x=df_prep['date'], y=[30]*len(df_prep), name="Oversold", line=dict(color="rgba(16, 185, 129, 0.5)", dash="dot"), showlegend=False), row=3, col=1)
                
                # Fix: Add a highly distinguished crimson Forecast Boundary Line across subplots without triggering shapeannotation bug
                fig.add_vline(
                    x=last_actual_date, 
                    line_width=2.5, 
                    line_dash="dash", 
                    line_color="#f43f5e"
                )
                
                # Fix: Add the text annotation separately using direct coordinate mapping (Bypasses Plotly's internal Timestamp mean() bug!)
                fig.add_annotation(
                    x=last_actual_date,
                    y=0.98,
                    yref="paper",
                    text="🔮 Forecast Boundary",
                    showarrow=False,
                    xanchor="right",
                    yanchor="top",
                    font=dict(color="#f43f5e", size=10, family="Space Grotesk")
                )
                
                fig.update_layout(
                    height=750, 
                    template="plotly_dark",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    xaxis_rangeslider_visible=False,
                    xaxis3_title="Timeline",
                    yaxis_title="Asset Price",
                    yaxis2_title="Volume",
                    yaxis3_title="RSI Index",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
                fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
                
                st.plotly_chart(fig, use_container_width=True)
                
            with tab2:
                st.markdown("### 🧠 Transformer Out-of-Sample Performance Backtest")
                st.markdown("Comparing the model's chronological next-day predictions against the true historical close price over the 20% validation window.")
                
                fig_val = go.Figure()
                
                fig_val.add_trace(go.Scatter(
                    x=val_dates, y=val_actuals_real,
                    name=f"True Price ({'Rupees' if currency_symbol == '₹' else 'Dollars'})",
                    line=dict(color="#e2e8f0", width=2)
                ))
                
                fig_val.add_trace(go.Scatter(
                    x=val_dates, y=val_preds_real,
                    name="Transformer Predictor (Stateless)",
                    line=dict(color="#60a5fa", width=2, dash="dash")
                ))
                
                fig_val.update_layout(
                    height=500,
                    template="plotly_dark",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    xaxis_title="Date",
                    yaxis_title="Price",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                fig_val.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
                fig_val.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
                
                st.plotly_chart(fig_val, use_container_width=True)
                
                # --- 🔮 NEON PATH FORECAST PROJECTION ---
                if k_steps > 1:
                    st.markdown("---")
                    st.markdown("### 🔮 Transformer Multi-Step Future Price Projection")
                    st.markdown(f"Displaying the projected price path up to **{target_dt.strftime('%d-%b-%Y')}** calculated via dynamic feature recalculation.")
                    
                    # Fetch last 60 days of historical actuals to connect smoothly
                    hist_dates = df_prep['date'].iloc[-60:]
                    hist_prices = df_prep['close'].iloc[-60:]
                    
                    fig_proj = go.Figure()
                    
                    # Recent actual prices
                    fig_proj.add_trace(go.Scatter(
                        x=hist_dates, y=hist_prices,
                        name="Recent Historical Prices",
                        line=dict(color="#94a3b8", width=2.5)
                    ))
                    
                    # Future projected path (neon-blue dashed)
                    fig_proj.add_trace(go.Scatter(
                        x=proj_dates_connected, y=proj_prices_connected,
                        name=f"Projected Forecast Path",
                        line=dict(color="#60a5fa", width=3, dash="dash")
                    ))
                    
                    # Fix: Add Forecast Boundary Line to Projection plot without annotation_text (Avoids Pandas Timestamp TypeError)
                    fig_proj.add_vline(
                        x=last_actual_date, 
                        line_width=2, 
                        line_dash="dash", 
                        line_color="#f43f5e"
                    )
                    
                    # Fix: Add the Forecast Boundary label separately on Tab 2
                    fig_proj.add_annotation(
                        x=last_actual_date,
                        y=0.98,
                        yref="paper",
                        text="🔮 Forecast Boundary",
                        showarrow=False,
                        xanchor="left",
                        yanchor="top",
                        font=dict(color="#f43f5e", size=10, family="Space Grotesk")
                    )
                    
                    fig_proj.update_layout(
                        height=500,
                        template="plotly_dark",
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        xaxis_title="Date Timeline",
                        yaxis_title="Asset Price",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    fig_proj.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
                    fig_proj.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
                    
                    st.plotly_chart(fig_proj, use_container_width=True)
            
            with tab3:
                st.markdown("### 🏢 Institution & Asset Profile")
                
                with st.spinner("Fetching institution details..."):
                    info_data = get_ticker_info(ticker_name)
                
                col_left, col_right = st.columns([2, 3])
                
                with col_left:
                    st.markdown(f"""
                        <div class="metric-card" style="margin-bottom: 20px;">
                            <div class="metric-label" style="font-size: 12px;">Full Legal Name</div>
                            <div style="font-size: 24px; font-weight: 700; color: #60a5fa; margin-top: 8px;">{info_data['name']}</div>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-label" style="font-size: 12px; margin-bottom: 12px;">Asset Profile</div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                                <span style="color: #94a3b8;">Ticker Symbol:</span>
                                <span style="font-weight: 600;">{symbol}</span>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                                <span style="color: #94a3b8;">Listing Exchange:</span>
                                <span style="font-weight: 600;">{exchange}</span>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                                <span style="color: #94a3b8;">Region/Country:</span>
                                <span style="font-weight: 600;">{country}</span>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                                <span style="color: #94a3b8;">Sector:</span>
                                <span style="font-weight: 600;">{info_data['sector']}</span>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                                <span style="color: #94a3b8;">Industry:</span>
                                <span style="font-weight: 600;">{info_data['industry']}</span>
                            </div>
                            <div style="display: flex; justify-content: space-between;">
                                <span style="color: #94a3b8;">Website:</span>
                                <span style="font-weight: 600;">{info_data['website']}</span>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                    
                with col_right:
                    st.markdown(f"""
                        <div class="metric-card" style="height: 100%;">
                            <div class="metric-label" style="font-size: 12px; margin-bottom: 12px;">Business Description</div>
                            <p style="color: #cbd5e1; font-size: 14px; line-height: 1.6; text-align: justify; margin: 0;">
                                {info_data['summary']}
                            </p>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            # Render Exchange Off / No Date selected screen
            if off_message == "EXCHANGE OFF":
                st.markdown("""
                    <div style="background-color: rgba(239, 68, 68, 0.05); border-radius: 16px; padding: 60px 40px; text-align: center; border: 1px solid rgba(239, 68, 68, 0.2); margin-top: 20px; backdrop-filter: blur(10px);">
                        <h1 style="color: #ef4444; font-size: 56px; font-weight: 800; margin: 0; letter-spacing: 2px; font-family: 'Space Grotesk', sans-serif;">EXCHANGE OFF</h1>
                        <p style="color: #cbd5e1; font-size: 20px; margin-top: 15px; font-family: 'Space Grotesk', sans-serif;">The selected target date falls on a weekend or an official market holiday.</p>
                        <p style="color: #64748b; font-size: 15px; margin-top: 10px;">Market data and forecasts are only available for valid exchange trading days.</p>
                    </div>
                """, unsafe_allow_html=True)
            elif off_message == "NO TARGET DATE SELECTED":
                st.markdown("""
                    <div style="background-color: rgba(255, 255, 255, 0.02); border-radius: 16px; padding: 60px 40px; text-align: center; border: 1px dashed rgba(255, 255, 255, 0.1); margin-top: 20px; backdrop-filter: blur(10px);">
                        <h1 style="color: #ffffff; font-size: 36px; font-weight: 700; margin: 0; font-family: 'Space Grotesk', sans-serif; letter-spacing: 1px;">NO TARGET DATE SELECTED</h1>
                        <p style="color: #cbd5e1; font-size: 18px; margin-top: 15px;">Please select a Future Forecast Target Date in the Configuration Panel to execute predictions.</p>
                    </div>
                """, unsafe_allow_html=True)
            else:
                # INVALID FORECAST DATE (PAST/TODAY)
                st.markdown(f"""
                    <div style="background-color: rgba(234, 179, 8, 0.05); border-radius: 16px; padding: 60px 40px; text-align: center; border: 1px solid rgba(234, 179, 8, 0.2); margin-top: 20px; backdrop-filter: blur(10px);">
                        <h1 style="color: #eab308; font-size: 36px; font-weight: 700; margin: 0; font-family: 'Space Grotesk', sans-serif;">{off_message}</h1>
                        <p style="color: #cbd5e1; font-size: 18px; margin-top: 15px;">The forecast target date must be set to a future date after the last closed price date ({last_actual_date.strftime('%d-%b-%Y')}).</p>
                    </div>
                """, unsafe_allow_html=True)
            
    else:
        st.error(f"⚠️ Failed to collect data for symbol {symbol}. Please check your ticker spelling or try another asset.")