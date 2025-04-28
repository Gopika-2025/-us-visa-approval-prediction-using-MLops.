import json
import sys
import os
import pandas as pd

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

from pandas import DataFrame

from US_Visa.exception import USvisaException
from US_Visa.logger import logging
from US_Visa.utils.main_utils import read_yaml_file, write_yaml_file
from US_Visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from US_Visa.entity.config_entity import DataValidationConfig
from US_Visa.constants import SCHEMA_FILE_PATH

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise USvisaException(e, sys)

    def validate_number_of_columns(self, dataframe: DataFrame) -> bool:
        try:
            status = len(dataframe.columns) == len(self._schema_config["columns"])
            logging.info(f"Is required column present: [{status}]")
            return status
        except Exception as e:
            raise USvisaException(e, sys)

    def is_column_exist(self, df: DataFrame) -> bool:
        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []
            missing_categorical_columns = []
            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)

            if missing_numerical_columns:
                logging.info(f"Missing numerical columns: {missing_numerical_columns}")

            for column in self._schema_config["categorical_columns"]:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)

            if missing_categorical_columns:
                logging.info(f"Missing categorical columns: {missing_categorical_columns}")

            return not (missing_numerical_columns or missing_categorical_columns)
        except Exception as e:
            raise USvisaException(e, sys)

    @staticmethod
    def read_data(file_path) -> DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise USvisaException(e, sys)

    def detect_dataset_drift(self, reference_df: DataFrame, current_df: DataFrame) -> bool:
        try:
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=reference_df, current_data=current_df)

            # Save report as HTML
            report_dir = os.path.dirname(self.data_validation_config.drift_report_file_path)
            os.makedirs(report_dir, exist_ok=True)
            report.save_html(self.data_validation_config.drift_report_file_path)

            # Analyze drift results (using report.json())
            report_json = report.as_dict()
            n_features = report_json["metrics"][0]["result"]["number_of_columns"]
            n_drifted_features = report_json["metrics"][0]["result"]["number_of_drifted_columns"]

            logging.info(f"{n_drifted_features}/{n_features} features drifted.")

            drift_detected = report_json["metrics"][0]["result"]["dataset_drift"]
            return drift_detected
        except Exception as e:
            raise USvisaException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            validation_error_msg = ""
            logging.info("Starting data validation")

            train_df = self.read_data(self.data_ingestion_artifact.trained_file_path)
            test_df = self.read_data(self.data_ingestion_artifact.test_file_path)

            # Column validation
            if not self.validate_number_of_columns(train_df):
                validation_error_msg += "Columns are missing in training dataframe. "
            if not self.validate_number_of_columns(test_df):
                validation_error_msg += "Columns are missing in testing dataframe. "
            if not self.is_column_exist(train_df):
                validation_error_msg += "Required columns are missing in training dataframe. "
            if not self.is_column_exist(test_df):
                validation_error_msg += "Required columns are missing in testing dataframe. "

            validation_status = len(validation_error_msg) == 0

            if validation_status:
                drift_status = self.detect_dataset_drift(train_df, test_df)
                if drift_status:
                    logging.info("Drift detected between training and testing data.")
                    validation_error_msg = "Data drift detected."
                else:
                    validation_error_msg = "No significant data drift detected."
            else:
                logging.info(f"Validation errors: {validation_error_msg}")

            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                message=validation_error_msg,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info(f"Data Validation Artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise USvisaException(e, sys)
