import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler
from sklearn.tree import DecisionTreeRegressor

from database.DatabaseUtil import DatabaseUtil
from database.models import Collisions


def predict_using_randon_forest(X_train, X_test, y_train, y_test):
    # Initialize and train a Random Forest Regressor model (MSE: 0.1493)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    # Make predictions on the testing set
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    print(f'Mean Squared Error: {mse}')
    return mse, predictions

def predict_using_linear_regression(X_train, X_test, y_train, y_test):
    # Initialize and train a Linear Regression model (MSE: 0.1581)
    model = LinearRegression()
    model.fit(X_train, y_train)
    # Make predictions on the testing set
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    print(f'Mean Squared Error: {mse}')
    return mse, predictions

def predict_using_neural_networks(X_train, X_test, y_train, y_test):
    # Scale the features for neural network training (MSE: 0.15262)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    # Initialize and train a Neural Network model
    nn_model = MLPRegressor(hidden_layer_sizes=(100, 50), activation='relu', solver='adam', random_state=42)
    nn_model.fit(X_train_scaled, y_train)
    # Make predictions on the scaled testing set
    nn_predictions = nn_model.predict(X_test_scaled)
    # Evaluate the model using Mean Squared Error (MSE)
    nn_mse = mean_squared_error(y_test, nn_predictions)
    print(f'Neural Network Mean Squared Error: {nn_mse}')
    return nn_mse, nn_predictions

def predict_using_knn(X_train, X_test, y_train, y_test):
    # Initialize and train a KNN model (MSE: 0.1904)
    knn_model = KNeighborsRegressor(n_neighbors=5)
    knn_model.fit(X_train, y_train)
    # Make predictions on the testing set
    knn_predictions = knn_model.predict(X_test)
    # Evaluate the model using Mean Squared Error (MSE)
    knn_mse = mean_squared_error(y_test, knn_predictions)
    print(f'KNN Mean Squared Error: {knn_mse}')
    return knn_mse, knn_predictions

def predict_using_decision_tree(X_train, X_test, y_train, y_test):
    # Initialize and train a Decision Tree model (MSE: 0.14626)
    tree_model = DecisionTreeRegressor(max_depth=5, random_state=42)
    tree_model.fit(X_train, y_train)
    # Make predictions on the testing set
    tree_predictions = tree_model.predict(X_test)
    # Evaluate the model using Mean Squared Error (MSE)
    tree_mse = mean_squared_error(y_test, tree_predictions)
    print(f'Decision Tree Mean Squared Error: {tree_mse}')
    return tree_mse, tree_predictions


# fetch data from postgres using sqlalchemy orm
session = DatabaseUtil.get_postgres_session()
sq = session.query(Collisions)
collisions = pd.read_sql(sq.statement, session.bind)
DatabaseUtil.close_postgres_session(session)

#EDA
collisions.hist(layout=(1, 9), figsize=(30, 5))
plt.show()

#categorical data to numbers
LE = LabelEncoder()
data = collisions.apply(LE.fit_transform)

#EDA HEATMAP
# # Calculate the correlation matrix
# correlation_matrix = data.corr()
# # Create a heatmap using Seaborn
# plt.figure(figsize=(10, 8))
# sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
# plt.title('Correlation Heatmap')
# plt.show()

# unsampling / Define independent variables (features) and dependent variable (target)
X = data[['crash_date', 'crash_time', 'on_street_name', 'cross_street_name', 'borough', 'contributing_factor_vehicle_1',
          'contributing_factor_vehicle_2', 'vehicle_type_code_1', 'vehicle_type_code_2', 'vehicle_type_code_3']]
y = data[['persons_injured', 'persons_killed', 'motorist_injured', 'motorist_killed', 'pedestrians_injured',
                                    'pedestrians_killed', 'cyclist_injured', 'cyclist_killed']]
# y = data[['NUMBER OF PERSONS KILLED', ]]

# Split the dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)

# Make predictions on the testing set
mse, predictions = predict_using_decision_tree(X_train, X_test, y_train, y_test)

# Plot actual values vs predictions
plt.figure(figsize=(10, 6))
plt.scatter(y_test, np.round(predictions), color='blue', label='Actual', alpha=0.5)
plt.scatter(y_test, np.round(predictions), color='yellow', label='Predicted', alpha=0.5)
plt.title('Actual vs Predicted Number of People Killed/Injured')
plt.xlabel('Actual Values')
plt.ylabel('Predicted Values')
plt.grid(True)
plt.legend()
plt.show()
print()














