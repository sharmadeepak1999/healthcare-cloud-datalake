import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import subprocess

def ml_electronic_health_record(ti):
    data = ti.xcom_pull(task_ids='transform_electronic_health_record', key='transformed_dataset')

    # Select features and target
    features = ['Total Charges', 'Total Costs', 'CCSR Procedure Code', 'CCSR Diagnosis Code', 'APR Severity of Illness Code']
    target = 'Length of Stay'

    X = data[features]
    y = data[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)

    command = "mlflow ui"
    process = subprocess.Popen(command, shell=True)

    # Set our tracking server uri for logging
    mlflow.set_tracking_uri(uri="http://127.0.0.1:5000")

    # Create a new MLflow Experiment
    mlflow.set_experiment("Length of Stay")

    # Define hyperparameters
    params = {'n_estimators': 100, 'min_samples_leaf': 10, 'max_features': 'sqrt', 'max_depth': 20}

    # Start MLflow run
    with mlflow.start_run(run_name='stay'):

        # Train Random Forest model
        model = RandomForestRegressor(**params)
        model.fit(X_train, y_train)

        # Make predictions
        y_pred = model.predict(X_test)

        # Calculate evaluation metrics
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_test, y_pred)

        # Log hyperparameters
        mlflow.log_params(params)

        # Log evaluation metrics
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)

        #printing the values
        print("| ", "Random Forest"," | ", r2," | ", mse, " | ",rmse, " | ")

        # Save model
        mlflow.sklearn.log_model(model, "random_forest_model")


        feature_names = X.columns.tolist()

        # Prepare a DataFrame for results
        result = pd.DataFrame(X_test, columns=feature_names)
        result["actual_Length_of_Stay"] = y_test
        result["predicted_Length_of_Stay"] = y_pred

        # Display the random four rows of the result
        print(result.sample(5))