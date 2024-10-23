import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from imblearn.over_sampling import SMOTE
import pickle




data = pd.read_csv("card_transdata.csv")



print(data.head())


X = data.drop('fraud', axis = 1) # Features
y = data['fraud'] # Target


# Handle Class imbalance
sm = SMOTE(random_state = 42)
X_res, y_res = sm.fit_resample(X, y)

# Split data into train and test

X_train, X_test, y_train, y_test = train_test_split(X_res, y_res, test_size=0.2, random_state=42)



# Model training
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)



# Evaluate model
y_pred = model.predict(X_test)

print(classification_report(y_test, y_pred))
print(f'ROC AUC Score : {roc_auc_score(y_test, y_pred)}')




# Save model to a pickle file

with open("fraud_detection.pkl", "wb") as model_file:
    pickle.dump(model, model_file)
