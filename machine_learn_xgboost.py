#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 16 22:59:25 2023

@author: bhkang
"""

import sqlite3
import pandas as pd
from sklearn.model_selection import  GridSearchCV
from sklearn.preprocessing import MinMaxScaler
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt

# Connect to the database
conn = sqlite3.connect('upbit_data.db')

# Get the data
table_names = ['KRW_ADA']
data = []

for table_name in table_names:
    query = f"SELECT open, high, low, close, volume FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    data.append(df)

# Merge the data
df_merged = pd.concat(data, axis=1)
df_merged.columns = [f"{table_name}_{col}" for table_name in table_names for col in ['open', 'high', 'low', 'close', 'volume']]

# Drop the ADA_USDT_close column from the features
X = df_merged.drop('KRW_ADA_close', 1)

# Keep the ADA_USDT_close column as the target
y = df_merged['KRW_ADA_close']

# Data normalization
scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X)

# Train-test split
train_size = int(0.9 * len(X_scaled))
X_train, X_test = X_scaled[:train_size], X_scaled[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

# XGBoost model
model = XGBRegressor(random_state=42)

# Hyperparameter grid
param_grid = {
    'n_estimators': [100, 200, 300],
    'max_depth': [3, 4, 5],
    'learning_rate': [0.1, 0.01, 0.001]
}

# GridSearchCV
grid_search = GridSearchCV(model, param_grid, scoring='r2', cv=3)
grid_search.fit(X_train, y_train)

# Best parameters and model
best_params = grid_search.best_params_
best_model = grid_search.best_estimator_

print('Best parameters:', best_params)

# Test set predictions
y_pred = best_model.predict(X_test)

# Evaluation
rmse = mean_squared_error(y_test, y_pred, squared=False)
r2 = r2_score(y_test, y_pred)

# Print additional information
additional_info = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
print(additional_info)

# Print results
print('RMSE:', rmse)
print('R-squared:', r2)


y_test = y_test.reset_index(drop=True)

plt.plot(y_test, marker='o', markersize=1, label='Actual')
plt.plot(y_pred, marker='o', markersize=1, label='Predicted')
plt.legend()
plt.title('Actual vs Predict 1')
plt.show()


plt.plot(y_test, marker='o', markersize=1, label='Actual')
plt.plot(y_pred, marker='o', markersize=1, label='Predicted')
plt.xlim(300, 350)
plt.ylim(380, 400)
# Add a legend
plt.legend()

# Set the title
plt.title('Actual vs Predict 2')

# Show the plot
plt.show()