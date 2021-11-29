def test_find_jwt_token():
    """Test that a deep token can be found"""
    assert False


def test_find_not_existing_jwt_token():
    """Test that a deep token can _not_ be found"""
    # should raise KeyError
    assert False


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    assert False


def test_jwt_get_token_still_valid():
    """Test that a jwt_token is returned if still valid"""
    assert False


def test_jwt_get_token_not_valid():
    """Test that a jwt_token is refreshed if no longer valid"""
    assert False


def test_jwt_get_token_storing():
    """Test that a jwt_token is stored in the Connections list"""
    assert False


def test_get_conn_returns_new():
    """Test that get_conn returns a new connection the first time"""
    assert False


def test_get_conn_returns_existing():
    """Test that get_conn returns an existing one if already created"""
    assert False


def test_get_conn_throws():
    """Test that get_conn throws if host, jwt_url or jwt_token_path are not present"""
    assert False
