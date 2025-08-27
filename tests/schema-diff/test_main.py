from main import app, get_hello_world


def test_get_hello_world():
    assert get_hello_world() == "Hello World"


def test_root_route():
    with app.test_client() as client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.data.decode() == "Hello World"
