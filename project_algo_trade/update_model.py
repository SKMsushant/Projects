import os
import sys
import time
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model

# Add parent directory to path to ensure clean imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from collection_data import Data_collection, stock_tups, stack_company_data
from preparation_data import Data_prep
from preprocessing_data import StatefulDataPreprocessor

def update_model():
    print(f"🔄 Starting scheduled model update at {pd.Timestamp.now()}...")
    
    # 1. Fetch latest raw data for all configured symbols (2020 to today)
    company_data = {}
    prep = Data_prep()
    
    for item in stock_tups:
        symbol, exchange, country, ownership, asset, yf_ticker = item
        collector = Data_collection(symbol, exchange, country, ownership, asset, yf_ticker)
        
        # end_date=None automatically downloads up to today's absolute latest candle!
        df = collector.collect_data(start_date='2020-01-01', end_date=None)
        if not df.empty:
            company_data[symbol] = df
        time.sleep(0.5) # Politeness delay
        
    if not company_data:
        print("❌ No market data could be downloaded. Aborting update.")
        return
        
    unified_df = stack_company_data(company_data)
    df_prepared = prep.prepare_features(unified_df)
    
    # 2. Preprocess & generate latest stateful sequences
    preprocessor = StatefulDataPreprocessor(seq_length=10, target_col='close')
    sequences, df_scaled = preprocessor.fit_transform(df_prepared)
    
    # 3. Load your existing high-capacity model
    model_path = os.path.join(current_dir, "best_stateful_gru.keras")
    if not os.path.exists(model_path):
        print(f"❌ Model file not found at {model_path}. Ensure best_stateful_gru.keras exists.")
        return
        
    model = load_model(model_path)
    
    # 4. Online Transfer Learning (Fine-tune weights for 2 epochs on latest sequence maps)
    print("⏳ Fine-tuning deep GRU layers on new market sequences...")
    for symbol, (X_train, y_train) in sequences.items():
        if len(X_train) == 0:
            continue
        
        # Reset recurrent state nodes before training a new asset history
        for layer in model.layers:
            if hasattr(layer, 'reset_states'):
                layer.reset_states()
                
        model.fit(
            X_train, y_train,
            epochs=2,
            batch_size=1,
            shuffle=False,
            verbose=0
        )
        
    # 5. Overwrite model with updated weights
    model.save(model_path)
    print(f"✅ Model successfully updated and saved to '{model_path}'!")

if __name__ == "__main__":
    update_model()