import numpy as np
import tensorflow as tf
import keras_tuner as kt
from tensorflow.keras.models import Sequential,load_model
from tensorflow.keras.layers import GRU, Dense, Dropout, LayerNormalization,Input
from tensorflow.keras.regularizers import l2
from tensorflow.keras.optimizers import Adam, AdamW, RMSprop
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import GRU, Dense, Dropout, LayerNormalization, Input
from tensorflow.keras.regularizers import l2

def build_stateful_gru(seq_length, num_features, batch_size=1, learning_rate=0.001, l2_reg=1e-4, dropout_rate=0.2):
    """
    Builds the exact same Stateful Deep GRU architecture used in application serving.
    """
    model = Sequential([
        Input(batch_shape=(batch_size, seq_length, num_features)),
        GRU(
            units=128,
            return_sequences=True,
            stateful=True,
            kernel_regularizer=l2(l2_reg),
            recurrent_regularizer=l2(l2_reg)
        ),
        LayerNormalization(),
        Dropout(dropout_rate),
        GRU(
            units=64,
            return_sequences=True,
            stateful=True,
            kernel_regularizer=l2(l2_reg),
            recurrent_regularizer=l2(l2_reg)
        ),
        LayerNormalization(),
        Dropout(dropout_rate),
        GRU(
            units=32,
            return_sequences=False,
            stateful=True,
            kernel_regularizer=l2(l2_reg),
            recurrent_regularizer=l2(l2_reg)
        ),
        LayerNormalization(),
        Dropout(dropout_rate),
        Dense(
            units=32,
            activation='relu',
            kernel_regularizer=l2(l2_reg)
        ),
        LayerNormalization(),
        Dropout(dropout_rate),
        Dense(
            units=16,
            activation='relu',
            kernel_regularizer=l2(l2_reg)
        ),
        LayerNormalization(),
        Dropout(dropout_rate),
        Dense(
            units=8,
            activation='relu',
            kernel_regularizer=l2(l2_reg)
        ),
        LayerNormalization(),
        Dropout(dropout_rate),
        Dense(units=1)
    ])
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=learning_rate, clipnorm=1.0),
        loss='mean_squared_error'
    )
    return model

# =====================================================================
# 1. Model Building
# =====================================================================
class StatefulGRUHyperModel(kt.HyperModel):
    def __init__(self, seq_length, num_features, batch_size=1):
        """
        Stateful GRU HyperModel. Defines the parameterized search space.
        """
        self.seq_length = seq_length
        self.num_features = num_features
        self.batch_size = batch_size

    def build(self, hp):
        """
        Builds and compiles the model dynamically based on HyperParameters (hp).
        """
        # --- Define Search Spaces ---
        # 1. L2 Regularization (continuous log search from 1e-5 to 1e-3)
        l2_reg = hp.Float('l2_reg', min_value=1e-5, max_value=1e-3, sampling='log')
        
        # 2. Dropout Rate (0.1 to 0.4 in steps of 0.1)
        dropout_rate = hp.Float('dropout_rate', min_value=0.1, max_value=0.4, step=0.1)
        
        
        # 4. Learning Rate (log search from 1e-4 to 1e-2)
        lr = hp.Float('learning_rate', min_value=1e-4, max_value=1e-2, sampling='log')

        model = Sequential([
            # 1. Keras 3 standard: Explicit Input layer with batch_shape for stateful tracking
            Input(batch_shape=(self.batch_size, self.seq_length, self.num_features)),
            
            # 2. GRU Layer 1 (Now clean of batch_input_shape!)
            GRU(
                units=128,
                return_sequences=True,
                stateful=True,
                kernel_regularizer=l2(l2_reg),
                recurrent_regularizer=l2(l2_reg)
            ),
            LayerNormalization(),
            Dropout(dropout_rate),
            
            # GRU Layer 2
            GRU(
                units=64,
                return_sequences=True,
                stateful=True,
                kernel_regularizer=l2(l2_reg),
                recurrent_regularizer=l2(l2_reg)
            ),
            LayerNormalization(),
            Dropout(dropout_rate),
            
            # GRU Layer 3
            GRU(
                units=32,
                return_sequences=False,
                stateful=True,
                kernel_regularizer=l2(l2_reg),
                recurrent_regularizer=l2(l2_reg)
            ),
            LayerNormalization(),
            Dropout(dropout_rate),
            
            # --- Dense Stack (4 Dense Layers) ---
            Dense(
                units=32,
                activation='relu',
                kernel_regularizer=l2(l2_reg)
            ),
            LayerNormalization(),
            Dropout(dropout_rate),
            
            Dense(
                units=16,
                activation='relu',
                kernel_regularizer=l2(l2_reg)
            ),
            LayerNormalization(),
            Dropout(dropout_rate),
            
            Dense(
                units=8,
                activation='relu',
                kernel_regularizer=l2(l2_reg)
            ),
            LayerNormalization(),
            Dropout(dropout_rate),
            
            Dense(units=1)
        ])
        
        # --- Compiler with Gradient Clipping ---
            
        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr,clipnorm=1.0), loss='mean_squared_error',metrics=['mean_squared_error'])
        return model

# =====================================================================
# 3. Custom Tuner Class
# =====================================================================

class StatefulChronologicalTuner(kt.Tuner):
    def run_trial(self, trial, train_sequences, val_sequences, epochs=3, batch_size=1):
        """
        Custom trial execution to run our symbol-by-symbol stateful training loop.
        Reports validation loss to the oracle for hyperparameter optimization!
        """
        # 1. Build the model for this trial config
        model = self.hypermodel.build(trial.hyperparameters)
        
        # 2. Run Stateful chronological training
        for epoch in range(epochs):
            # Train per symbol
            for symbol, (X_train, y_train) in train_sequences.items():
                if len(X_train) == 0:
                    continue
                for layer in model.layers:
                    if hasattr(layer, 'reset_states'):
                        layer.reset_states()
                model.fit(
                    X_train, y_train,
                    epochs=1,
                    batch_size=batch_size,
                    shuffle=False,
                    verbose=0
                )
                
        # 3. Evaluate on validation sequences per symbol
        val_losses = []
        for symbol, (X_val, y_val) in val_sequences.items():
            if len(X_val) == 0:
                continue
            for layer in model.layers:
                if hasattr(layer, 'reset_states'):
                    layer.reset_states()
            val_loss = model.evaluate(
                X_val, y_val,
                batch_size=batch_size,
                verbose=0
            )
            val_losses.append(val_loss)
            
        mean_val_loss = np.mean(val_losses)
        
        # 4. Report the objective metric (mean validation loss) to the tuner oracle
        self.oracle.update_trial(trial.trial_id, {"val_loss": mean_val_loss})
        self.save_model(trial.trial_id, model)
    # =================================================================
    # ✅ ADDED: Implement save_model for Keras Tuner Base compatibility
    # =================================================================
    def save_model(self, trial_id, model, step=0):
        import os
        # Ensure project folder exists
        os.makedirs(self.project_dir, exist_ok=True)
        # Create standard filepath
        filepath = os.path.join(self.project_dir, f"trial_{trial_id}")
        model.save(f"{filepath}.keras")
    # =================================================================
    # ✅ ADDED: Implement load_model for Keras Tuner Base compatibility
    # =================================================================
    def load_model(self, trial):
        import os
        filepath = os.path.join(self.project_dir, f"trial_{trial.trial_id}")
        return load_model(f"{filepath}.keras")
    
    

# =====================================================================
# 3. Main Pipeline Execution (Runs when executing: python model.py)
# =====================================================================
if __name__ == "__main__":
    import time
    
    # Import your pre-written custom modules!
    from collection_data import Data_collection, stock_tups, stack_company_data
    from preparation_data import Data_prep
    from preprocessing_data import StatefulDataPreprocessor
    
    print("\n" + "="*50)
    print("📈 ALGORITHMIC TRADING SYSTEM - TRAINING PIPELINE 📈")
    print("="*50 + "\n")
    
    # -----------------------------------------------------------------
    # STEP 1: Ingest Raw Historical Data (2020 to 2025)
    # -----------------------------------------------------------------
    print("⏳ Step 1: Collecting historical stock and index data (5-Year Window)...")
    company_data = {}
    
    for item in stock_tups:
        symbol, exchange, country, ownership, asset, yf_ticker = item
        collector = Data_collection(symbol, exchange, country, ownership, asset, yf_ticker)
        
        # Download from 2020-01-01 to 2025-01-01 for high-capacity training
        df = collector.collect_data(start_date='2020-01-01', end_date='2025-01-01')
        if not df.empty:
            company_data[symbol] = df
            
        time.sleep(0.5) # Politeness delay to prevent rate limits
        
    unified_df = stack_company_data(company_data)
    print(f"✅ Ingestion complete. Combined shape: {unified_df.shape}")
    
    # -----------------------------------------------------------------
    # STEP 2: Add Technical Indicators
    # -----------------------------------------------------------------
    print("\n⏳ Step 2: Calculating short and medium-term technical indicators...")
    prep = Data_prep()
    df_prepared = prep.prepare_features(unified_df)
    print(f"✅ Feature Engineering complete. Prepared shape: {df_prepared.shape}")
    
    # -----------------------------------------------------------------
    # STEP 3: Advanced Preprocessing & Sequence Generation
    # -----------------------------------------------------------------
    print("\n⏳ Step 3: Performing ordinal encoding, lifetime scaling, and sequence building...")
    # Use a sequence length of 10 days for memory-efficient stateful tracking
    preprocessor = StatefulDataPreprocessor(seq_length=10, target_col='close')
    sequences, df_scaled = preprocessor.fit_transform(df_prepared)
    
    # -----------------------------------------------------------------
    # STEP 4: Chronological Train / Validation Split per Stock
    # -----------------------------------------------------------------
    print("\n⏳ Step 4: Splitting sequences chronologically per stock (80% Train, 20% Val)...")
    train_sequences = {}
    val_sequences = {}
    
    for symbol, (X, y) in sequences.items():
        # Chronological boundary split (no shuffling to prevent leakage)
        split_idx = int(len(X) * 0.8)
        
        train_sequences[symbol] = (X[:split_idx], y[:split_idx])
        val_sequences[symbol] = (X[split_idx:], y[split_idx:])
        
        print(f"   ▪ {symbol:<10} | Train samples: {len(X[:split_idx]):<5} | Val samples: {len(X[split_idx:]):<5}")
        
    # Dynamically extract sequence specs from our preprocessed tensors
    first_symbol = list(sequences.keys())[0]
    sample_X, _ = sequences[first_symbol]
    num_features = sample_X.shape[2]
    seq_length = sample_X.shape[1]
    
    print(f"\n🔢 GRU Input Dimensions -> Timesteps (Days): {seq_length} | Features: {num_features}")
    
    # -----------------------------------------------------------------
    # STEP 5: Initialize Keras Tuner with Bayesian Optimization
    # -----------------------------------------------------------------
    print("\n⏳ Step 5: Initializing Keras Tuner (Bayesian Optimization)...")
    hypermodel = StatefulGRUHyperModel(seq_length=seq_length, num_features=num_features, batch_size=1)
    
    tuner = StatefulChronologicalTuner(
        oracle=kt.oracles.BayesianOptimizationOracle(
            objective=kt.Objective("val_loss", direction="min"),
            max_trials=5,  # We will test 5 optimized combinations
            seed=42
        ),
        hypermodel=hypermodel,
        directory="tuner_results",
        project_name="algo_trade_stateful_gru"
    )
    
    # -----------------------------------------------------------------
    # STEP 6: Execute Hyperparameter Search
    # -----------------------------------------------------------------
    print("\n🚀 Step 6: Launching stateful hyperparameter optimization...")
    tuner.search(
        train_sequences=train_sequences,
        val_sequences=val_sequences,
        epochs=3, # Run 3 epochs per trial for rapid search evaluation
        batch_size=1
    )
    
    # -----------------------------------------------------------------
    # STEP 7: Retrieve and Save Best Performing Model
    # -----------------------------------------------------------------
    print("\n" + "="*50)
    print("🏆 PIPELINE RUN COMPLETE & CONVERGED!")
    print("="*50)
    
    best_model = tuner.get_best_models(num_models=1)[0]
    best_hp = tuner.get_best_hyperparameters(1)[0]
    
    print("✨ Best Optimizer: ADAM (Static)")
    print(f"✨ Best Learning Rate: {best_hp.get('learning_rate')}")
    print(f"✨ Best L2 Weight Decay: {best_hp.get('l2_reg')}")
    print(f"✨ Best Dropout Probability: {best_hp.get('dropout_rate')}")
    
    # Save the best weights
    best_model.save("best_stateful_gru.keras")
    print("\n💾 Best model architecture and weights successfully saved to 'best_stateful_gru.keras'!")