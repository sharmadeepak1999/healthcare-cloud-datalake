import pandas as pd
from sklearn.preprocessing import LabelEncoder
import os

def map_and_fill_missing_values(dataframe, from_col, to_col):
   temp_df = dataframe.copy()

   map = temp_df.groupby(from_col)[to_col].agg(
      lambda x: x.value_counts().idxmax() if x.count() > 0 else None
   ).to_dict()
    
   temp_df[to_col] = temp_df[to_col].fillna(temp_df[from_col].map(map))
   
   return temp_df


def impute_missing_values_with_mode(dataframe, column, missing_value=None):
    temp_df = dataframe.copy()
    mode_value = temp_df[column].mode()[0]
    temp_df[column] = temp_df[column].replace(missing_value, mode_value)
    return temp_df


def detect_and_impute_outliers(dataframe, column):
    temp_df = dataframe.copy()

    q1 = dataframe[column].quantile(0.25)
    q3 = dataframe[column].quantile(0.75)
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outlier_count = temp_df[(temp_df[column] < lower_bound) | (temp_df[column] > upper_bound)].shape[0]
    print(f"Number of outliers in {column}: {outlier_count}")

    temp_df[column] = temp_df[column].mask(temp_df[column] < lower_bound, lower_bound, axis=0)
    temp_df[column] = temp_df[column].mask(temp_df[column] > upper_bound, upper_bound, axis=0)
    return temp_df


def one_hot_encoding(dataframe, columns):
    encoded_df = pd.get_dummies(dataframe, columns=columns)
    return encoded_df



def transform_electronic_health_record(ti):
    df = ti.xcom_pull(task_ids='read_dataset', key='dataset')

    df = df.drop_duplicates()

    missing_values_percentage = (df.isnull().mean() * 100)
    columns_to_drop = missing_values_percentage[missing_values_percentage > 50].index
    df = df.drop(columns_to_drop, axis=1)

    df = df.drop(['Operating Certificate Number','Discharge Year'], axis = 1)

    df = map_and_fill_missing_values(df, "CCSR Diagnosis Code", "CCSR Procedure Code")
    df = map_and_fill_missing_values(df, "CCSR Procedure Code", "CCSR Procedure Description")

    df = df.dropna(axis=0)

    df = impute_missing_values_with_mode(df, "Gender", missing_value="U")
    df = impute_missing_values_with_mode(df, "Type of Admission", missing_value="Not Available")

    mode_race_for_unknown_ethinicity = df[df["Ethnicity"] == "Unknown"]["Race"].mode()[0]
    mode_ethinicity_for_selection = df[df["Race"] == mode_race_for_unknown_ethinicity]["Ethnicity"].mode()[0]
    df["Ethnicity"] = df["Ethnicity"].replace("Unknown", mode_ethinicity_for_selection)

    df["Length of Stay"] = df["Length of Stay"].replace("120 +", "120").astype(int)
    df["Zip Code - 3 digits"] = df["Zip Code - 3 digits"].replace("OOS", "999").astype(int)
    df["Permanent Facility Id"] = df["Permanent Facility Id"].astype(int)
    df["Total Charges"] = df["Total Charges"].replace(",", "", regex=True).astype(float)
    df["Total Costs"] = df["Total Costs"].replace(",", "", regex=True).astype(float)

    df = detect_and_impute_outliers(df, "Total Charges")
    df = detect_and_impute_outliers(df, "Total Costs")

    redundant_columns = ["Facility Name", "CCSR Diagnosis Description", "CCSR Procedure Description", "APR DRG Description", "APR MDC Description", "APR Severity of Illness Description"]
    df = df.drop(redundant_columns, axis=1)


    target_encoding_columns = ["Hospital Service Area", "Hospital County", "Patient Disposition", "CCSR Diagnosis Code", "CCSR Procedure Code"]
    for col in target_encoding_columns:
        df[col] = df[col].map(dict(df.groupby(col)["Length of Stay"].mean()))

    df['Age Group'] = LabelEncoder().fit_transform(df['Age Group'])

    df['APR Risk of Mortality'] = df['APR Risk of Mortality'].map({'Minor': 0, 'Moderate': 1, 'Major': 2, 'Extreme': 3})

    temp_dict = dict(df['Payment Typology 1'].value_counts() / len(df))
    df['Payment Typology 1'] = df['Payment Typology 1'].map(temp_dict)

    one_hot_encoding_columns = ["Gender", "Race", "Ethnicity", "Type of Admission", "APR Medical Surgical Description", "Emergency Department Indicator"]
    df = one_hot_encoding(df, one_hot_encoding_columns)

    df.to_csv(os.path.join(os.environ.get('AIRFLOW_HOME'), "datasets/electronic-health-record/output/transformed_electronic_health_record_dataset.csv"), index=False)
    ti.xcom_push(key='transformed_dataset', value=df)