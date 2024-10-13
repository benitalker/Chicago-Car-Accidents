import pytest
from flask import Flask

from controller.accidents_controller import accidents_controller
from controller.initialize_database import initialize_database


@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(initialize_database)
    app.register_blueprint(accidents_controller)

    with app.test_client() as client:
        yield client


def test_initialize_db(client):
    response = client.get('/initialize_db')
    assert response.status_code == 200
    assert response.json == {'message': 'Database initialized successfully.'}



def test_accidents_by_area(client):
    response = client.get('/accidents_by_area?area=225')
    assert response.status_code == 200
