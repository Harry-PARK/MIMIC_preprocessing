import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, roc_auc_score, accuracy_score, precision_score, recall_score
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

continuous = np.load("../archive/earlyAgg/real_earlyAgg_continuous.npy")
discrete = np.load("..archive/earlyAgg/real_earlyAgg_discrete.npy")
mortality_label = np.load("../archive/earlyAgg/real_earlyAgg_IN_HOSPITAL_MORTALITY_label.npy")

# Combine continuous and discrete features
X = np.concatenate((continuous, discrete), axis=1)
y = mortality_label

# Impute NaNs
imputer = SimpleImputer(strategy='mean')
X = imputer.fit_transform(X)

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Scale features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Initialize and train the LogisticRegression model
model = LogisticRegression(random_state=42, class_weight='balanced', solver='liblinear')
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

# Calculate metrics
f1 = f1_score(y_test, y_pred)
auroc = roc_auc_score(y_test, y_pred_proba)
accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)

# Print the results
print(f"F1 Score: {f1:.4f}")
print(f"AUROC: {auroc:.4f}")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")

