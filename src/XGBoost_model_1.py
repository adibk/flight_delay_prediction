import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
import xgboost as xgb
import joblib

import load_data

verbose = True
joblib_file = "XGBoost_pipeline.pkl"
model_path="model"

con, cur = load_data.connect_to_database('flight_prediction')
df = load_data.load_data(con)
df['delayed'] = df['arr_delay'].apply(lambda x: 1 if x > 0 else 0)
df = df.drop(columns=['arr_delay'])

def extract_features(df):
    df['day_of_week'] = df['date'].dt.dayofweek
    df['dep_hour'] = df['dep_time'] // 100
    df['arr_hour'] = df['arr_time'] // 100
    return df.drop(columns=['date', 'dep_time', 'arr_time'])

X = df.drop(columns=['delayed'])
y = df['delayed']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

pipeline = Pipeline(steps=[
    ('feature_engineering', FunctionTransformer(extract_features, validate=False)),
    ('scaler', StandardScaler()),
    ('classifier', xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss'))
])
pipeline.fit(X_train, y_train)

joblib.dump(pipeline, f"{model_path}/{joblib_file}")
loaded_pipeline = joblib.load(f"{model_path}/{joblib_file}")
y_pred = loaded_pipeline.predict(X_test)

if verbose:
    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("Classification Report:\n", classification_report(y_test, y_pred))