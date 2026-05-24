import pandas as pd
import numpy as np

class Data_prep:
    def __init__(self, period_rsi=14, period_mfi=14, period_bb=20):
        self.period_rsi = period_rsi
        self.period_mfi = period_mfi
        self.period_bb = period_bb

    def _add_ema(self, df):
        # 3-day and 5-day EMA
        df['ema_3'] = df.groupby('symbol')['close'].transform(lambda x: x.ewm(span=3, adjust=False).mean())
        df['ema_5'] = df.groupby('symbol')['close'].transform(lambda x: x.ewm(span=5, adjust=False).mean())
        return df

    def _add_macd(self, df):
        # MACD (12, 26, 9)
        df['ema_12'] = df.groupby('symbol')['close'].transform(lambda x: x.ewm(span=12, adjust=False).mean())
        df['ema_26'] = df.groupby('symbol')['close'].transform(lambda x: x.ewm(span=26, adjust=False).mean())
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df.groupby('symbol')['macd'].transform(lambda x: x.ewm(span=9, adjust=False).mean())
        # Drop temporary EMAs to keep features clean
        df = df.drop(columns=['ema_12', 'ema_26'])
        return df

    def _add_rsi(self, df):
        def calc_rsi(series):
            delta = series.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=self.period_rsi).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=self.period_rsi).mean()
            rs = gain / (loss + 1e-9)
            return 100 - (100 / (1 + rs))

        df['rsi_14'] = df.groupby('symbol')['close'].transform(calc_rsi).fillna(50)
        return df

    def _add_bollinger_bands(self, df):
        rolling_mean = df.groupby('symbol')['close'].transform(lambda x: x.rolling(window=self.period_bb).mean())
        rolling_std = df.groupby('symbol')['close'].transform(lambda x: x.rolling(window=self.period_bb).std())
        
        df['bollinger_mid'] = rolling_mean
        df['bollinger_hband'] = rolling_mean + (rolling_std * 2)
        df['bollinger_lband'] = rolling_mean - (rolling_std * 2)
        
        # Fill leading NaNs with current price to prevent scaling issues
        df['bollinger_mid'] = df['bollinger_mid'].fillna(df['close'])
        df['bollinger_hband'] = df['bollinger_hband'].fillna(df['close'])
        df['bollinger_lband'] = df['bollinger_lband'].fillna(df['close'])
        return df

    def _add_vwap(self, df):
        df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_vol'] = df['typical_price'] * df['volume']
        
        # Vectorized cumsum per symbol — 10x faster and immune to MultiIndex bugs!
        df['vwap'] = df.groupby('symbol')['tp_vol'].transform('cumsum') / (
            df.groupby('symbol')['volume'].transform('cumsum') + 1e-9
        )
        
        # Drop temp columns
        df = df.drop(columns=['typical_price', 'tp_vol'])
        return df

    def _add_mfi(self, df):
        """
        Calculates Money Flow Index (MFI) per symbol using 100% vectorized operations.
        Completely avoids slow, buggy .apply() loops!
        """
        # 1. Row-wise Typical Price
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        
        # 2. Row-wise Raw Money Flow
        df['raw_mf'] = df['tp'] * df['volume']
        
        # 3. Typical Price Difference per symbol (prevents boundary bleed between stocks)
        df['tp_diff'] = df.groupby('symbol')['tp'].diff()
        
        # 4. Positive and Negative Money Flows
        df['pos_mf'] = df['raw_mf'].where(df['tp_diff'] > 0, 0)
        df['neg_mf'] = df['raw_mf'].where(df['tp_diff'] < 0, 0)
        
        # 5. Rolling sums of positive and negative flows per symbol (boundary-safe!)
        pos_flow_sum = df.groupby('symbol')['pos_mf'].transform(
            lambda x: x.rolling(window=self.period_mfi).sum()
        )
        neg_flow_sum = df.groupby('symbol')['neg_mf'].transform(
            lambda x: x.rolling(window=self.period_mfi).sum()
        )
        
        # 6. Calculate Money Flow Index
        money_ratio = pos_flow_sum / (neg_flow_sum + 1e-9)
        df['mfi_14'] = 100 - (100 / (1 + money_ratio))
        
        # 7. Clean up temporary columns to keep dataframe pristine
        df = df.drop(columns=['tp', 'raw_mf', 'tp_diff', 'pos_mf', 'neg_mf'])
        
        # Fill leading NaNs with neutral 50
        df['mfi_14'] = df['mfi_14'].fillna(50)
        return df

    def prepare_features(self, df):
        """
        Calculates all technical indicators on the fly.
        """
        df = df.copy().sort_values(by=['symbol', 'date']).reset_index(drop=True)
        
        df = self._add_ema(df)
        df = self._add_macd(df)
        df = self._add_rsi(df)
        df = self._add_bollinger_bands(df)
        df = self._add_vwap(df)
        df = self._add_mfi(df)
        
        # Drop initial NaN rows created by rolling windows (e.g. first 20 rows of each stock)
        df = df.dropna().reset_index(drop=True)
        return df

if __name__ == "__main__":
    # Test with mock stacked data
    # (Assuming unified_df is generated from Step 1)
    print("Testing data preparation features engineering...")