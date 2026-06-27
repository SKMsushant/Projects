import os
import sys
import time
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model
import json
import keras_tuner as kt

# Add parent directory to path to ensure clean imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from collection_data import Data_collection, stock_tups, stack_company_data
from preparation_data import Data_prep
from preprocessing_data import StatefulDataPreprocessor
from model import Transformer_Encoder, Static_Positional_Embedding, Transformer_Encoder_Model

def update_model():
    print(f"Starting scheduled model update at {pd.Timestamp.now()}...")
    
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
        print("No market data could be downloaded. Aborting update.")
        return
        
    unified_df = stack_company_data(company_data)
    df_prepared = prep.prepare_features(unified_df)
    
    # 2. Preprocess & generate latest sequences
    preprocessor = StatefulDataPreprocessor(seq_length=10, target_col='close')
    sequences, df_scaled = preprocessor.fit_transform(df_prepared)
    
    # 3. Rebuild the Transformer model and load weights
    hps_path = os.path.join(current_dir, "best_hps.json")
    weights_path = os.path.join(current_dir, "best_transformer_weights.weights.h5")
    
    if not os.path.exists(hps_path) or not os.path.exists(weights_path):
        print("Configuration or weight files not found. Ensure model.py has run successfully.")
        return
        
    with open(hps_path, "r") as f:
        hps = json.load(f)
        
    hp = kt.HyperParameters()
    for k, v in hps.items():
        hp.values[k] = v
        
    seq_len = 10
    num_features = 24
    hypermodel = Transformer_Encoder_Model(seq_len=seq_len, num_features=num_features)
    model = hypermodel.build(hp)
    model.load_weights(weights_path)
    
    # 4. Concatenate new sequences globally for stateless batch training
    print("Fine-tuning Transformer on new market sequences...")
    X_train_list = []
    y_train_list = []
    
    for symbol, (X_train, y_train) in sequences.items():
        if len(X_train) == 0:
            continue
        X_train_list.append(X_train)
        y_train_list.append(y_train)
        
    if not X_train_list:
        print("No sequence data available for training.")
        return
        
    X_train_global = np.concatenate(X_train_list, axis=0)
    y_train_global = np.concatenate(y_train_list, axis=0)
    
    # 5. Online Transfer Learning (Fine-tune weights for 2 epochs on the global matrix)
    batch_size = hps.get("Batch_size", 32)
    model.fit(
        X_train_global, y_train_global,
        epochs=2,
        batch_size=batch_size,
        shuffle=True,
        verbose=1
    )
        
    # 6. Save updated weights back
    model.save_weights(weights_path)
    print(f"Model weights successfully updated and saved to '{weights_path}'!")
if __name__ == "__main__":
    update_model()