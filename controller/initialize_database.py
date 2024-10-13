from flask import Blueprint, jsonify
from service.initialize_database_service import load_accident_data_service

initialize_database = Blueprint('injury_statistics', __name__)

@initialize_database.route('/initialize_db', methods=['POST'])
def initialize_db():
    try:
        load_accident_data_service()
        return jsonify({"message": "Database initialized successfully."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
