from flask import Blueprint, jsonify, request
from repository.accident_repository import (
    get_accidents_by_week,
    get_accidents_by_month, get_accidents_by_day, get_accidents_grouped_by_cause, get_accidents_by_area)
from service.accident_service import convert_object_id

accidents_controller = Blueprint('accidents', __name__)

@accidents_controller.route('/accidents_by_area', methods=['GET'])
def accidents_by_area():
    try:
        area = request.args.get('area')

        if not area:
            return jsonify({"error": "Area is required"}), 400

        data = get_accidents_by_area(area)

        return jsonify(convert_object_id(data)) if data else jsonify({"message": "No data found for the given area"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint for accidents by day
@accidents_controller.route('/accidents_by_day', methods=['GET'])
def accidents_by_day():
    try:
        area = request.args.get('area')
        day = request.args.get('day')  # Expecting format 'YYYY-MM-DD'

        if not area or not day:
            return jsonify({"error": "Area and day are required"}), 400

        data = get_accidents_by_day(area, day)
        return (jsonify(convert_object_id(data)) if data
                else  jsonify({"message": "No data found for the given area and day"}), 404)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint for accidents by week
@accidents_controller.route('/accidents_by_week', methods=['GET'])
def accidents_by_week():
    try:
        area = request.args.get('area')
        week_start = request.args.get('week_start')  # Expecting format 'YYYY-MM-DD'

        if not area or not week_start:
            return jsonify({"error": "Area, week_start are required"}), 400

        data = get_accidents_by_week(area, week_start)

        return (jsonify(convert_object_id(data)) if data
                else jsonify({"message": "No data found for the given area and week"}), 404)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint for accidents by month
@accidents_controller.route('/accidents_by_month', methods=['GET'])
def accidents_by_month():
    try:
        area = request.args.get('area')
        month = request.args.get('month')  # Expecting format 'YYYY-MM'

        if not area or not month:
            return jsonify({"error": "Area and month are required"}), 400

        data = get_accidents_by_month(area, month)

        return (jsonify(convert_object_id(data)) if data
                else jsonify({"message": "No data found for the given area and month"}), 404)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint for accidents grouped by cause
@accidents_controller.route('/accidents_by_cause', methods=['GET'])
def accidents_by_cause():
    try:
        area = request.args.get('area')

        if not area:
            return jsonify({"error": "Area is required"}), 400

        data = get_accidents_grouped_by_cause(area)

        return (jsonify(convert_object_id(data)) if data
                else jsonify({"message": "No data found for the given area"}), 404)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
