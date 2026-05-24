import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import tensorflow as tf
from tensorflow.keras.models import load_model
import os
import time

# Import your pre-written custom modules!
from collection_data import Data_collection, stock_tups, stack_company_data
from preparation_data import Data_prep
from preprocessing_data import StatefulDataPreprocessor


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
        model_path = os.path.join(current_dir, "best_stateful_gru.keras")
        model = load_model(model_path)
        return model
    except Exception as e:
        st.error(f"❌ Failed to load 'best_stateful_gru.keras'. Ensure you ran model.py first! Error: {e}")
        return None

def reset_model_states(model):
    """Keras 3 safe model state reset."""
    for layer in model.layers:
        if hasattr(layer, 'reset_states'):
            layer.reset_states()

st.sidebar.markdown("## 📊 Configuration Panel")

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
st.sidebar.markdown("### 🔮 Future Forecast Horizon")
tomorrow = pd.to_datetime("today") + pd.Timedelta(days=1)
forecast_target_date = st.sidebar.date_input("Forecast Target Date", value=tomorrow)

# Upgraded selector: Toggle between Raw GRU and Blended Technical Momentum mode
forecast_mode = st.sidebar.selectbox(
    "Forecast Model Mode",
    options=[
        "🚀 Blended GRU + Technical Momentum (Dynamic)",
        "🎯 Pure Stateful GRU (Current Static Weights)"
    ]
)

st.markdown("<h1 style='text-align: center; font-weight: 700; margin-bottom: 2px;'>⚡ QUANT ENGINE</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #94a3b8; font-size: 16px; margin-bottom: 30px;'>Stateful Deep GRU Algorithmic Trading Core</p>", unsafe_allow_html=True)

model = load_trained_model()

if model is not None:
    # 1. Resolve selected ticker and metadata
    if selected_symbol != "Custom Ticker":
        # Pull metadata from stock_tups
        tup = [t for t in stock_tups if t[0] == selected_symbol][0]
        symbol, exchange, country, ownership, asset, ticker_name = tup
    else:
        symbol = custom_ticker.upper()
        exchange, country, ownership, asset, ticker_name = "GLOBAL", "Global", "Private", "Equity", custom_ticker
        
    with st.spinner(f"⏳ Syncing real-time market data for {symbol}..."):
        # 2. Ingest latest data
        collector = Data_collection(symbol, exchange, country, ownership, asset, ticker_name)
        df_raw = collector.collect_data(start_date=start_date.strftime('%Y-%m-%d'), end_date=end_date.strftime('%Y-%m-%d'))
        
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
        target_dt = pd.to_datetime(forecast_target_date)
        
        # Identify business days to forecast
        future_bdays = pd.bdate_range(start=last_actual_date + pd.Timedelta(days=1), end=target_dt)
        k_steps = len(future_bdays)
        
        # Calculate Tomorrow's Price prediction (Standard Baseline)
        latest_seq = X_all[-1:] # Shape: (1, 10, 23)
        reset_model_states(model)
        tomorrow_pred_scaled = model.predict(latest_seq, batch_size=1, verbose=0)[0, 0]
        
        dummy_pred = np.zeros((1, len(scaler.scale_)))
        dummy_pred[0, target_idx] = tomorrow_pred_scaled
        tomorrow_pred_real = scaler.inverse_transform(dummy_pred)[0, target_idx]
        
        # Calculate last actual price
        today_actual_real = df_prep['close'].iloc[-1]
        
        # Resolve target prediction details
        if k_steps <= 1:
            k_steps = 1
            forecast_label = "Tomorrow"
            target_pred_real = tomorrow_pred_real
            future_preds = [(last_actual_date + pd.Timedelta(days=1), tomorrow_pred_real)]
        else:
            forecast_label = f"{target_dt.strftime('%d-%b-%Y')}"
            
            if "Blended" in forecast_mode:
                # 🚀 DYNAMIC MODE: Blended Technical Momentum Interpolation
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
                # 🎯 STATIC MODE: Pure Stateful GRU (Current Static Weights)
                df_forecast = df_raw.copy().sort_values('date').reset_index(drop=True)
                rolling_avg_volume = df_raw['volume'].iloc[-10:].mean()
                future_preds = []
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for step in range(k_steps):
                    fut_date = future_bdays[step]
                    status_text.text(f"🔮 Stateful GRU forecasting step {step + 1}/{k_steps}...")
                    
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
            
        # B. Calculate last actual price and shift metrics
        price_diff = target_pred_real - today_actual_real
        pct_change = (price_diff / today_actual_real) * 100
        
        # C. Predict Validation Set (for backtest visualization)
        reset_model_states(model)
        val_preds_scaled = model.predict(X_val, batch_size=1, verbose=0)
        
        dummy_val_preds = np.zeros((len(val_preds_scaled), len(scaler.scale_)))
        dummy_val_preds[:, target_idx] = val_preds_scaled.flatten()
        val_preds_real = scaler.inverse_transform(dummy_val_preds)[:, target_idx]
        
        dummy_val_actuals = np.zeros((len(y_val), len(scaler.scale_)))
        dummy_val_actuals[:, target_idx] = y_val
        val_actuals_real = scaler.inverse_transform(dummy_val_actuals)[:, target_idx]
        
        # Calculate Validation Accuracy Metrics
        val_dates = df_prep['date'].iloc[split_idx + 10:].values # align sequence index offset
        
        # Directional Accuracy Calculation
        today_val_actuals = []
        for seq in X_val:
            today_val_actuals.append(seq[-1, target_idx])
            
        dummy_today = np.zeros((len(today_val_actuals), len(scaler.scale_)))
        dummy_today[:, target_idx] = today_val_actuals
        today_val_real = scaler.inverse_transform(dummy_today)[:, target_idx]
        
        actual_direction = np.sign(val_actuals_real - today_val_real)
        predicted_direction = np.sign(val_preds_real - today_val_real)
        directional_acc = np.mean(actual_direction == predicted_direction) * 100
        
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Last Closed Price</div>
                    <div class="metric-value">₹{today_actual_real:,.2f}</div>
                </div>
            """, unsafe_allow_html=True)
            
        with col2:
            direction_class = "text-up" if price_diff >= 0 else "text-down"
            arrow = "▲" if price_diff >= 0 else "▼"
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Forecast ({forecast_label})</div>
                    <div class="metric-value">₹{target_pred_real:,.2f}</div>
                </div>
            """, unsafe_allow_html=True)
            
        with col3:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Projected Shift</div>
                    <div class="metric-value {direction_class}">{arrow} {pct_change:+.2f}%</div>
                </div>
            """, unsafe_allow_html=True)
            
        with col4:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">GRU Directional Accuracy</div>
                    <div class="metric-value" style="color: #60a5fa;">{directional_acc:.1f}%</div>
                </div>
            """, unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        

        tab1, tab2 = st.tabs(["📈 Market Candlestick & Indicators", "🧠 GRU Model Backtest Analysis"])
        
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
                name="GRU Forecast Path", 
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
            st.markdown("### 🧠 GRU Out-of-Sample Performance Backtest")
            st.markdown("Comparing the model's chronological next-day predictions against the true historical close price over the 20% validation window.")
            
            fig_val = go.Figure()
            
            fig_val.add_trace(go.Scatter(
                x=val_dates, y=val_actuals_real,
                name="True Price (Rupees/Dollars)",
                line=dict(color="#e2e8f0", width=2)
            ))
            
            fig_val.add_trace(go.Scatter(
                x=val_dates, y=val_preds_real,
                name="GRU Predictor (Stateful)",
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
                st.markdown("### 🔮 Stateful GRU Multi-Step Future Price Projection")
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
            
    else:
        st.error(f"⚠️ Failed to collect data for symbol {symbol}. Please check your ticker spelling or try another asset.")