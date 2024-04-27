import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
import os
from scipy import stats

def univariate_analysis(data, columns,title="Overall Univariate Analysis"):
    plt.figure(figsize=(20,10 * len(columns) // 2))
    rows = (len(columns) + 1) // 2
    ind = 1

    sns.set_style("whitegrid")
    plt.suptitle(title, fontsize=25)
    plt.subplots_adjust(hspace=0.5, wspace=0.25)
    
    for column in columns:
        plt.subplot(rows, 2, ind)
        
        if data[column].dtype == "object":
            sns.countplot(x=data[column], hue=data[column], order=data[column].value_counts().index, hue_order=data[column].value_counts().index, palette="crest")
        else:
            sns.histplot(x=data[column], color="#37888d")
            
        plt.title(f"Distribution of {column}")
        plt.ylabel("Frequency")
        plt.xticks(rotation=90)
        ind += 1
        
    file_path = os.path.join(os.environ.get('AIRFLOW_HOME'), "analytics/electronic_health_record/insights/plots/")
    file_name = re.sub("[^a-zA-Z0-9]", "_", title.lower())
    
    plt.savefig(file_path + file_name, bbox_inches="tight", pad_inches=0.5)


def bivariate_analysis(df):
    plt.figure(figsize=(20, 20))
    sns.set_style("whitegrid")
    plt.suptitle("Bivariate Analysis", fontsize=25)
    plt.subplots_adjust(hspace=0.75, wspace=0.25)

    plt.subplot(2, 2, 1)
    sns.scatterplot(data=df, x='Length of Stay', y='Total Charges')
    plt.title('Length of Stay vs. Total Charges')
    plt.xlabel('Length of Stay (days)')
    plt.ylabel('Total Charges (USD)')


    plt.subplot(2, 2, 2)
    sns.boxplot(data=df, x='Patient Disposition', y='Total Charges')
    plt.title('Patient Disposition vs. Total Charges')
    plt.xlabel('Patient Disposition')
    plt.xticks(rotation=90)
    plt.ylabel('Total Charges (USD)')


    plt.subplot(2, 2, 3)
    sns.boxplot(data=df, x='Hospital Service Area', y='Total Charges')
    plt.title('Hospital Service Area vs. Total Charges')
    plt.xlabel('Hospital Service Area')
    plt.xticks(rotation=90)
    plt.ylabel('Total Charges (USD)')


    plt.subplot(2, 2, 4)
    sns.countplot(data=df, x='APR Severity of Illness Description', hue='Gender')
    plt.title('APR Severity of Illness by Gender')
    plt.xlabel('Severity of Illness Description')
    plt.ylabel('Count')

    file_path = os.path.join(os.environ.get('AIRFLOW_HOME'), "analytics/electronic_health_record/insights/plots/")
    file_name = "bivariate_analysis"
    plt.savefig(file_path + file_name, bbox_inches="tight", pad_inches=0.5)


def analyse_electronic_health_record(ti):
    df = ti.xcom_pull(task_ids='transform_electronic_health_record', key='transformed_dataset_for_analysis')

    columns_for_analysis = ["Length of Stay", "Total Charges", "Hospital Service Area", "Payment Typology 1"]
    univariate_analysis(df, columns_for_analysis)

    random_hospital_id = 1458
    columns_for_analysis = ["Length of Stay", "Total Charges", "Type of Admission", "Payment Typology 1"]
    univariate_analysis(df[df["Permanent Facility Id"] == random_hospital_id], columns_for_analysis, title=f"Univariate Analysis for Facility Id: {random_hospital_id}")


    bivariate_analysis(df)