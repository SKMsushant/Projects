import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

class StatefulDataPreprocessor:
    def __init__(self, seq_length=10, target_col='close'):
        """
        
        Args:
            seq_length (int): The sequence length (timesteps) per batch.
            target_col (str): The column to predict ('close').
        """
        self.seq_length = seq_length
        self.target_col = target_col
        self.categorical_cols = ['exchange', 'country', 'ownership_type', 'asset_type']
        
        # Encoders & Scalers
        self.encoding_maps = {}
        self.symbol_scalers = {}
        self.feature_cols = []

    def _add_lifetime_expanding_features(self, df):
        """
        INJECTS THE WHOLE PAST HISTORY:
        Computes cumulative, expanding metrics from Day 1 to Day T for each stock.
        """
        df = df.copy().sort_values(by=['symbol', 'date']).reset_index(drop=True)
        
        # 1. Cumulative Return since Inception
        # Formula: (Price_t - Price_Inception) / Price_Inception
        df['cum_return_lifetime'] = df.groupby('symbol')['close'].transform(
            lambda x: (x - x.iloc[0]) / (x.iloc[0] + 1e-9)
        )
        
        # 2. Lifetime Expanding Max (Resistance)
        df['expanding_max_lifetime'] = df.groupby('symbol')['close'].transform(
            lambda x: x.cummax()
        )
        
        # 3. Lifetime Expanding Min (Support)
        df['expanding_min_lifetime'] = df.groupby('symbol')['close'].transform(
            lambda x: x.cummin()
        )
        
        # 4. Lifetime Expanding Volatility (Expanding Std Dev of returns)
        df['pct_change'] = df.groupby('symbol')['close'].pct_change().fillna(0)
        df['expanding_vol_lifetime'] = df.groupby('symbol')['pct_change'].transform(
            lambda x: x.expanding().std().fillna(0)
        )
        
        df = df.drop(columns=['pct_change'])
        return df

    def fit_target_guided_ordinal(self, df):
        """
        Fits Target-Guided Ordinal Encoding on categories using lifetime returns.
        """
        df = df.copy()
        df['pct_return'] = df.groupby('symbol')['close'].pct_change().fillna(0)
        
        for col in self.categorical_cols:
            category_means = df.groupby(col)['pct_return'].mean().sort_values()
            encoding_map = {category: rank for rank, category in enumerate(category_means.index)}
            self.encoding_maps[col] = encoding_map

    def transform_categorical(self, df):
        df = df.copy()
        for col in self.categorical_cols:
            mapping = self.encoding_maps.get(col, {})
            df[col] = df[col].map(mapping).fillna(0).astype(int)
        return df

    def scale_numerical_per_symbol(self, df, numerical_cols, is_training=True):
        """
        Scales numerical features (including our lifetime cumulative features!) per stock.
        """
        df = df.copy()
        scaled_dfs = []
        
        for symbol, group in df.groupby('symbol'):
            group = group.copy()
            if is_training:
                scaler = MinMaxScaler(feature_range=(0, 1))
                group[numerical_cols] = scaler.fit_transform(group[numerical_cols])
                self.symbol_scalers[symbol] = scaler
            else:
                if symbol in self.symbol_scalers:
                    group[numerical_cols] = self.symbol_scalers[symbol].transform(group[numerical_cols])
                else:
                    scaler = MinMaxScaler(feature_range=(0, 1))
                    group[numerical_cols] = scaler.fit_transform(group[numerical_cols])
                    self.symbol_scalers[symbol] = scaler
            scaled_dfs.append(group)
            
        return pd.concat(scaled_dfs, axis=0).sort_values(by=['symbol', 'date']).reset_index(drop=True)

    def create_stateful_sequences(self, df, feature_cols):
        """
        Generates sequences symbol-by-symbol chronologically.
        """
        symbol_sequences = {}
        
        for symbol, group in df.groupby('symbol'):
            group_sorted = group.sort_values('date').reset_index(drop=True)
            
            features_arr = group_sorted[feature_cols].values
            target_arr = group_sorted[self.target_col].values
            
            X, y = [], []
            for i in range(len(group_sorted) - self.seq_length):
                X.append(features_arr[i : i + self.seq_length])
                y.append(target_arr[i + self.seq_length])
                
            symbol_sequences[symbol] = (np.array(X), np.array(y))
            
        return symbol_sequences

    def fit_transform(self, df):
        """
        Complete standard pipeline for training a single copy.
        """
        # 1. Inject lifetime history features (expanding support, resistance, cumulative returns)
        df_lifetime = self._add_lifetime_expanding_features(df)
        
        # 2. Fit and Transform categories (Target-Guided Ordinal)
        self.fit_target_guided_ordinal(df_lifetime)
        df_encoded = self.transform_categorical(df_lifetime)
        
        # 3. Identify numerical columns (including the new expanding features)
        exclude_cols = ['date', 'symbol'] + self.categorical_cols
        numerical_cols = [col for col in df_encoded.columns if col not in exclude_cols]
        
        # 4. Scale numerical columns per symbol
        df_scaled = self.scale_numerical_per_symbol(df_encoded, numerical_cols, is_training=True)
        
        # 5. Define final features
        self.feature_cols = numerical_cols + self.categorical_cols
        
        # 6. Generate chronological stateful sequences grouped by symbol
        sequences = self.create_stateful_sequences(df_scaled, self.feature_cols)
        
        return sequences, df_scaled

    def fit_transform_dual(self, df):
        """
        GENERATES TWO PREPROCESSED DATA COPIES:
        Copy A (Traditional Baseline): Excludes 'news_sentiment' column.
        Copy B (Sentiment-Enhanced): Includes 'news_sentiment' column (used for application).
        """
        # --- COPY A: Traditional Price & Technical Indicators Only ---
        df_a = df.drop(columns=['news_sentiment'], errors='ignore').copy()
        preprocessor_a = StatefulDataPreprocessor(seq_length=self.seq_length, target_col=self.target_col)
        sequences_a, scaled_a = preprocessor_a.fit_transform(df_a)
        
        # --- COPY B: Sentiment-Enhanced (Appended with News Sentiment) ---
        df_b = df.copy()
        preprocessor_b = StatefulDataPreprocessor(seq_length=self.seq_length, target_col=self.target_col)
        sequences_b, scaled_b = preprocessor_b.fit_transform(df_b)
        
        # Copy properties of the sentiment-enhanced preprocessor to this instance
        # so that subsequent app calls use the sentiment-enhanced scaling map
        self.encoding_maps = preprocessor_b.encoding_maps
        self.symbol_scalers = preprocessor_b.symbol_scalers
        self.feature_cols = preprocessor_b.feature_cols
        self.preprocessor_a = preprocessor_a
        self.preprocessor_b = preprocessor_b
        
        return (sequences_a, scaled_a),(sequences_b, scaled_b)