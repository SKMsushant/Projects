import os
import datetime
import numpy as np
import tensorflow as tf
import keras_tuner as kt
from tensorflow.keras.models import Sequential,load_model
from tensorflow.keras.layers import MultiHeadAttention, Dense, Dropout, LayerNormalization,Input,GlobalAveragePooling1D
from tensorflow.keras.optimizers import Adam, AdamW, RMSprop
import json

# =====================================================================
# 1. Transformer Encoder Class
# =====================================================================

class Transformer_Encoder(tf.keras.layers.Layer):

    def __init__(self,num_heads,d_model,ff_dim,dropout=0.1,kernel_regularizer=None,**kwargs):
        super().__init__(**kwargs)

        self.mha=tf.keras.layers.MultiHeadAttention(num_heads=num_heads,key_dim=d_model,value_dim=d_model)
        self.ffn=tf.keras.Sequential([
            tf.keras.layers.Dense(ff_dim,activation='relu',kernel_regularizer=kernel_regularizer),
            tf.keras.layers.Dense(d_model,kernel_regularizer=kernel_regularizer)
        ])
        self.layernorm_mha=tf.keras.layers.LayerNormalization(epsilon=1e-6)
        self.layernorm_ffn=tf.keras.layers.LayerNormalization(epsilon=1e-6)
        self.dropout_mha=tf.keras.layers.Dropout(dropout)
        self.dropout_ffn=tf.keras.layers.Dropout(dropout)


    def call(self,x,training=False):
        #Multi-Head Attention and residual connection
        attn_output=self.mha(x,x,x,training=training)
        attn_output=self.dropout_mha(attn_output,training=training)
        out1=self.layernorm_mha(x+attn_output)

        #Feed Forward network and residual connection
        ffn_output=self.ffn(out1,training=training)
        ffn_output=self.dropout_ffn(ffn_output,training=training)
        return self.layernorm_ffn(out1+ffn_output)
    

# =====================================================================
# 2. Positional Embedding Class
# =====================================================================
    
class Static_Positional_Embedding(tf.keras.layers.Layer):
    def __init__(self,seq_len,d_model,**kwargs):
        super().__init__(**kwargs)
        self.seq_len=seq_len
        self.d_model=d_model


        sentence_positional_vector_list=[]

        for word_position in range(self.seq_len):

            word_position_vector=[]
            for i in range(int((self.d_model/2))):
                y_sin=np.sin(word_position/10000**((2*i)/self.d_model))
                y_cos=np.cos(word_position/10000**((2*i)/self.d_model))
                word_position_vector.append(y_sin)
                word_position_vector.append(y_cos)

            if len(word_position_vector)<self.d_model:
                word_position_vector.append(np.sin(word_position/10000**((self.d_model-1)/self.d_model)))

            sentence_positional_vector_list.append(word_position_vector)

        encoding_matrix= np.array(sentence_positional_vector_list)
        self.positional_encoding=tf.constant(encoding_matrix,dtype=tf.float32)[None,...]

    def call(self,x):
        return x+self.positional_encoding

# =====================================================================
# 3. Model Builder Class
# =====================================================================

class Transformer_Encoder_Model(kt.HyperModel):
    def __init__(self,seq_len,num_features,num_heads=4,ff_dim=256,dropout_rate=0.2):
        super().__init__()
        self.seq_len=seq_len
        self.num_features=num_features
        self.num_heads=num_heads
        self.ff_dim=ff_dim
        self.dropout_rate=dropout_rate
        self.model=None

    #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    def build(self,hp):
        #=======================================================

        reg_type=hp.Choice('reg_type',['l1','l2','l1_l2','None'])
        if reg_type=='l1':
            reg=tf.keras.regularizers.L1(
                l1=hp.Float('l1',min_value=1e-8,max_value=1e-3,sampling='log')
            )
        elif reg_type=='l2':
            reg=tf.keras.regularizers.L2(
                l2=hp.Float('l2',min_value=1e-8,max_value=1e-3,sampling='log')
            )
        elif reg_type=='l1_l2':
            reg=tf.keras.regularizers.L1L2(
                l1=hp.Float('l1',min_value=1e-8,max_value=1e-3,sampling='log'),
                l2=hp.Float('l2',min_value=1e-8,max_value=1e-3,sampling='log')
            )
        else:
            reg=None

        #=============================================================    



        inputs=tf.keras.layers.Input(shape=(self.seq_len,self.num_features))

        pos_encoding=Static_Positional_Embedding(seq_len=self.seq_len,d_model=self.num_features)(inputs)

        transformer_block=Transformer_Encoder(
            num_heads=self.num_heads,
            d_model=self.num_features,
            ff_dim=self.ff_dim,
            dropout=hp.Float('dropout_rate',min_value=0.1,max_value=0.4,step=0.1)
        )

        x=transformer_block(pos_encoding)

        x=GlobalAveragePooling1D()(x)
        x=Dropout(self.dropout_rate)(x)
        x=Dense(32,activation='relu',kernel_regularizer=reg)(x)
        outputs=Dense(1)(x)

        self.model=tf.keras.Model(inputs=inputs,outputs=outputs,name='Transformer_Stock_Predictor')
        lr=hp.Float('Learning_Rate',min_value=1e-4,max_value=1e-2,sampling='log')
        self.model.compile(
            optimizer=AdamW(learning_rate=lr),
            loss=tf.keras.losses.MeanSquaredError(),
            metrics=[tf.keras.metrics.R2Score(name='r2_score')]
        )

        return self.model
    
    #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    
    def fit(self,hp,model,x,y,validation_data,*args,**kwargs):


        project_dir = os.path.dirname(os.path.abspath(__file__))
        model_save_path=os.path.join(project_dir,r'\predicting_models')
        if not os.path.exists(model_save_path):
            os.makedirs(model_save_path,exist_ok=True)
        
        now=datetime.datetime.now()
        timestamp=now.strftime('[%Y_%m_%d_%H_%M_%S]')


        model_checkpoint=tf.keras.callbacks.ModelCheckpoint(
            filepath=os.path.join(model_save_path,f"{timestamp}_best_hypertransformer_weights.weights.h5"),
            monitor='val_loss',
            verbose=1,
            save_best_only=True,
            save_weights_only=True,
            mode='min',
            save_freq='epoch'
        )

        early_stopping=tf.keras.callbacks.EarlyStopping(
            monitor='val_loss',
            min_delta=1e-5,
            patience=10,
            verbose=1,
            mode='min',
            restore_best_weights=True
        )

        reduce_lr=tf.keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.2,
            patience=3,
            min_lr=1e-6,
            verbose=1
        )

        callbacks=kwargs.pop('callbacks',[])
        callbacks.append(model_checkpoint)
        callbacks.append(early_stopping)
        callbacks.append(reduce_lr)


        x_val, y_val = validation_data
        epochs=hp.Int('epochs',min_value=10,max_value=30,step=5)
        batch_size=hp.Int('Batch_size',min_value=16,max_value=64,step=8)
        return model.fit(
            x=x,
            y=y,
            epochs=epochs,
            validation_data=(x_val, y_val),
            callbacks=callbacks,
            batch_size=batch_size,
            verbose='auto',
            **kwargs
        )
    #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    def predict(self, X, batch_size=32, verbose=0):
        
        if self.model is None:
            raise ValueError("Model must be built before making predictions.")
        return self.model.predict(X, batch_size=batch_size, verbose=verbose)
        
    def save(self, filepath):
       
        if self.model is None:
            raise ValueError("Model must be built before saving.")
        self.model.save(filepath)


    
    

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
    print("TRANFORMER STOCK PREDICTOR - TRAINING PIPELINE ")
    print("="*50 + "\n")
    
    # -----------------------------------------------------------------
    # STEP 1: Ingest Raw Historical Data (2020 to 2025)
    # -----------------------------------------------------------------
    print(" Step 1: Collecting historical stock and index data (5-Year Window)...")
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
    print(f" Ingestion complete. Combined shape: {unified_df.shape}")
    
    # -----------------------------------------------------------------
    # STEP 2: Add Technical Indicators
    # -----------------------------------------------------------------
    print("\n Step 2: Calculating short and medium-term technical indicators...")
    prep = Data_prep()
    df_prepared = prep.prepare_features(unified_df)
    print(f" Feature Engineering complete. Prepared shape: {df_prepared.shape}")
    
    # -----------------------------------------------------------------
    # STEP 3: Advanced Preprocessing & Sequence Generation
    # -----------------------------------------------------------------
    print("\n Step 3: Performing ordinal encoding, lifetime scaling, and sequence building...")
    # Use a sequence length of 10 days for memory-efficient stateful tracking
    preprocessor = StatefulDataPreprocessor(seq_length=10, target_col='close')
    sequences, df_scaled = preprocessor.fit_transform(df_prepared)
    
    # -----------------------------------------------------------------
    # STEP 4: Chronological Train / Validation Split per Stock
    # -----------------------------------------------------------------
    print("\n Step 4: Splitting sequences chronologically per stock (80% Train, 20% Val)...")
    train_X_list, train_y_list = [], []
    val_X_list, val_y_list = [], []
    
    for symbol, (X, y) in sequences.items():
        # Chronological boundary split (no shuffling to prevent leakage)
        split_idx = int(len(X) * 0.8)
        
        train_X_list.append(X[:split_idx])
        train_y_list.append(y[:split_idx])
        val_X_list.append(X[split_idx:])
        val_y_list.append(y[split_idx:])
        
        print(f" {symbol:<10} | Train samples: {len(X[:split_idx]):<5} | Val samples: {len(X[split_idx:]):<5}")

    # Concatenate all tickers into global datasets
    X_train_global = np.concatenate(train_X_list, axis=0)
    y_train_global = np.concatenate(train_y_list, axis=0)
    X_val_global = np.concatenate(val_X_list, axis=0)
    y_val_global = np.concatenate(val_y_list, axis=0)
        
    # Dynamically extract sequence specs from our preprocessed tensors
    first_symbol = list(sequences.keys())[0]
    sample_X, _ = sequences[first_symbol]
    num_features = sample_X.shape[2]
    seq_length = sample_X.shape[1]
    
    print(f"\n Input Dimensions -> Timesteps (Days): {seq_length} | Features: {num_features}")
    
    # -----------------------------------------------------------------
    # STEP 5: Initialize Keras Tuner with Bayesian Optimization
    # -----------------------------------------------------------------
    print("\n Step 5: Initializing Keras Tuner (Bayesian Optimization)...")
    hypermodel = Transformer_Encoder_Model(seq_len=seq_length, num_features=num_features,)
    
    tuner = kt.BayesianOptimization(
        hypermodel=hypermodel,
        objective=kt.Objective("val_loss", direction="min"),
        max_trials=5,
        executions_per_trial=1,
        directory="tuner_results",
        project_name="algo_trade_transformer"
    )
    
    # -----------------------------------------------------------------
    # STEP 6: Execute Hyperparameter Search
    # -----------------------------------------------------------------
    print("\n Step 6: Launching stateful hyperparameter optimization...")
    tuner.search(
        x=X_train_global,
        y=y_train_global,
        validation_data=(X_val_global,y_val_global),
        )
    
    # -----------------------------------------------------------------
    # STEP 7: Retrieve and Save Best Performing Model
    # -----------------------------------------------------------------
    print("\n" + "="*50)
    print(" PIPELINE RUN COMPLETE & CONVERGED!")
    print("="*50)
    
    best_model = tuner.get_best_models(num_models=1)[0]
    best_hp = tuner.get_best_hyperparameters(1)[0]
    

    print(f" Best Learning Rate: {best_hp.get('Learning_Rate')}")
    print(f" Best Dropout Rate: {best_hp.get('dropout_rate')}")
    print(f" Best Reg Type: {best_hp.get('reg_type')}")
    
    best_hps_dict = best_hp.values
    best_hps_filepath = "best_hps.json"
    with open(best_hps_filepath, "w") as f:
        json.dump(best_hps_dict, f, indent=4)
    print(f" Best hyperparameters saved to '{best_hps_filepath}'")
    
    # Save the best model's weights to best_transformer_weights.h5
    weights_filepath = "best_transformer_weights.weights.h5"
    best_model.save_weights(weights_filepath)
    print(f" Best weights successfully saved to '{weights_filepath}'!")