from flask import Blueprint, jsonify, request

from repository.statistics_repository import get_injury_statistics_by_area
from service.accident_service import convert_object_id

# Change name to something unique
injury_statistics_controller = Blueprint('injury_statistics_controller', __name__)

@injury_statistics_controller.route('/injury_statistics', methods=['GET'])
def injury_statistics():
    try:
        area = request.args.get('area')

        if not area:
            return jsonify({"error": "Missing required parameter: area"}), 400

        result = get_injury_statistics_by_area(area)

        return (jsonify(convert_object_id(result)) if result
            else jsonify({"error": "No injury statistics found for the specified area"}), 404)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
