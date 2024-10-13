from repository.csv_repository import initialize_database


def load_accident_data_service():
    initialize_database()
    print("Accident data loaded and processed successfully.")


